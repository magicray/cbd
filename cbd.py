import os
import time
import uuid
import json
import boto3
import fcntl
import struct
import socket
import sqlite3
import logging
import hashlib
import argparse
import lz4.block
import threading
import traceback
from logging import critical as log


def panic(msg):
    log(msg)
    traceback.print_exc()
    os._exit(1)


def device_init(dev, block_size, block_count, conn):
    # Value = (0xab << 8) + n
    # Network Block Device ioctl commands
    NBD_SET_SOCK = 43776
    NBD_SET_BLKSIZE = 43777
    # NBD_SET_SIZE = 43778
    NBD_DO_IT = 43779
    NBD_CLEAR_SOCK = 43780
    NBD_CLEAR_QUEUE = 43781
    NBD_PRINT_DEBUG = 43782
    NBD_SET_SIZE_BLOCKS = 43783
    NBD_DISCONNECT = 43784
    NBD_SET_TIMEOUT = 43785
    NBD_SET_FLAGS = 43786

    fd = os.open(dev, os.O_RDWR)
    fcntl.ioctl(fd, NBD_CLEAR_QUEUE)
    fcntl.ioctl(fd, NBD_DISCONNECT)
    fcntl.ioctl(fd, NBD_CLEAR_SOCK)
    fcntl.ioctl(fd, NBD_SET_BLKSIZE, block_size)
    fcntl.ioctl(fd, NBD_SET_SIZE_BLOCKS, block_count)
    fcntl.ioctl(fd, NBD_SET_TIMEOUT, 60)
    fcntl.ioctl(fd, NBD_PRINT_DEBUG)
    fcntl.ioctl(fd, NBD_SET_SOCK, conn)

    # set options : WRITE_ZEROS(64), DISCARD(32), FLUSH(4)
    fcntl.ioctl(fd, NBD_SET_FLAGS, 64+32+4+1)

    log('initialized(%s) block_size(%d) block_count(%d)',
        dev, block_size, block_count)

    # Block forever
    fcntl.ioctl(fd, NBD_DO_IT)


class S3:
    def __init__(self, endpoint, bucket, prefix):
        self.prefix = prefix
        self.bucket = bucket
        self.endpoint = endpoint

        if self.endpoint:
            self.s3 = boto3.client('s3', endpoint_url=self.endpoint)
        else:
            os.makedirs(os.path.join(bucket, prefix, 'log'), exist_ok=True)
            os.makedirs(os.path.join(bucket, prefix, 'index'), exist_ok=True)

    def put(self, key, value):
        ts = time.time()
        key = os.path.join(self.prefix, key)

        if self.endpoint:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=value)
        else:
            tmpfile = os.path.join(self.bucket, uuid.uuid4().hex)
            with open(tmpfile, 'wb') as fd:
                fd.write(value)
            os.rename(tmpfile, os.path.join(self.bucket, key))

        log('bucket(%s/%s) put(%s) length(%d) msec(%d)',
            self.endpoint, self.bucket, key, len(value),
            (time.time()-ts) * 1000)

    def get(self, key):
        ts = time.time()
        key = os.path.join(self.prefix, key)

        if self.endpoint:
            try:
                obj = self.s3.get_object(Bucket=self.bucket, Key=key)
                octets = obj['Body'].read()
                assert (len(octets) == obj['ContentLength'])
            except self.s3.exceptions.NoSuchKey:
                octets = ''
        else:
            path = os.path.join(self.bucket, key)
            if os.path.isfile(path):
                with open(path, 'rb') as fd:
                    octets = fd.read()
            else:
                octets = ''

        log('bucket(%s/%s) get(%s) length(%d) msec(%d)',
            self.endpoint, self.bucket, key, len(octets),
            (time.time()-ts) * 1000)

        return octets if octets else None


def backup(log_seq_num, logdir):
    s3 = S3(ARGS.s3endpoint, ARGS.s3bucket, ARGS.s3prefix)

    while True:
        lsn = log_seq_num + 1

        lsnfile = os.path.join(logdir, str(lsn))
        if not os.path.isfile(lsnfile):
            time.sleep(1)
            continue

        with open(lsnfile, 'rb') as fd:
            octets = fd.read()

        s3.put(f'log/{lsn}', octets)
        s3.put('tail.json', json.dumps(dict(lsn=lsn)).encode())

        log_seq_num = lsn


def recvall(conn, length):
    buf = list()

    remaining = length
    while remaining:
        octets = conn.recv(length)

        if not octets:
            conn.close()
            raise Exception('connection closed')

        buf.append(octets)
        remaining -= len(octets)

    octets = b''.join(buf)

    if length != len(octets):
        panic(f'recvall length({length}) mismatch({len(octets)})')

    return octets


def server(sock, block_size, device_block_count, logdir, log_seq_num, db):
    db = sqlite3.connect(db)
    s3 = S3(ARGS.s3endpoint, ARGS.s3bucket, ARGS.s3prefix)

    conn, peer = sock.accept()
    log('client connection accepted')

    zero_block = lz4.block.compress(bytearray(block_size))

    fds = dict()
    logs = dict()

    stats = dict(ts=time.time(), read=0, write=0, discard=0)

    while True:
        magic, flags, cmd, cookie, req_offset, req_length = struct.unpack(
            '!IHHQQI', recvall(conn, 28))

        ts = time.time()

        if 0x25609513 != magic:
            panic(f'invalid magic({magic}) or cmd({cmd})')

        if 0 != req_offset % block_size or 0 != req_length % block_size:
            panic(f'invalid offset({req_offset}) or length({req_length})')

        # Response header is common. No errors are supported.
        response_header = struct.pack('!IIQ', 0x67446698, 0, cookie)

        block_count = req_length // block_size
        block_offset = req_offset // block_size

        # READ
        if 0 == cmd:
            blocks = list()

            rows = db.execute('''select block, lsn, offset, length from blocks
                                 where block between ? and ?
                              ''', [block_offset, block_offset+block_count])
            if rows:
                rows = {r[0]: (r[1], r[2], r[3]) for r in rows.fetchall()}

            for i in range(block_offset, block_offset+block_count):
                block = zero_block

                if i in logs:
                    block = logs[i][1]
                else:
                    if i in rows:
                        lsn, index, length = rows[i]
                    else:
                        lsn, index, length = 0, 0, 0

                    if lsn:
                        download_lsnfile(s3, lsn)

                    lsn_file = os.path.join(logdir, str(lsn))
                    if os.path.isfile(lsn_file):
                        if lsn_file not in fds:
                            fds[lsn_file] = os.open(lsn_file, os.O_RDONLY)

                        octets = os.pread(fds[lsn_file], length+64, index)

                        # header : block_index, lsn, usec_timestamp, length
                        hdr = octets[:32]
                        num, logseq, usec, length = struct.unpack('!QQQQ', hdr)

                        # this is the actual data
                        block = octets[32:-32]

                        # checksum - sha256
                        chksum = octets[-32:]

                        if num != i or lsn != logseq:
                            panic(('corrupt block', i, num, lsn, logseq, usec))

                        sha = hashlib.sha256(hdr)
                        sha.update(block)
                        if sha.digest() != chksum:
                            log(block)
                            panic(('corrupt block', num, lsn, usec, chksum))

                blocks.append(lz4.block.decompress(block))

            octets = b''.join(blocks)

            if len(octets) != req_length:
                panic(f'read length({req_length}) mismatch({len(octets)})')

            stats['read'] += block_count
            conn.sendall(response_header)
            conn.sendall(octets)

        # WRITE
        if 1 == cmd:
            for i in range(block_offset, block_offset+block_count):
                octets = lz4.block.compress(recvall(conn, block_size))

                hdr = struct.pack('!QQQQ', i, log_seq_num+1,
                                  int(ts*1000000), len(octets))

                sha = hashlib.sha256(hdr)
                sha.update(octets)

                logs[i] = [hdr, octets, sha.digest()]

            stats['write'] += block_count

        # DISCARD
        if 4 == cmd:
            rows = db.execute('''select block, lsn, offset, length from blocks
                                 where block between ? and ?
                              ''', [block_offset, block_offset+block_count])
            if rows:
                rows = {r[0]: (r[1], r[2], r[3]) for r in rows.fetchall()}

            for i in range(block_offset, block_offset+block_count):
                if i not in logs and i not in rows:
                    continue

                hdr = struct.pack('!QQQQ', i, log_seq_num+1,
                                  int(ts*1000000), len(zero_block))

                sha = hashlib.sha256(hdr)
                sha.update(zero_block)

                logs[i] = [hdr, zero_block, sha.digest()]

            stats['discard'] += block_count

        # FLUSH
        if (3 == cmd and len(logs) > 0) or len(logs) > 4096:
            log_seq_num += 1

            i = 0
            deletes = list()
            inserts = list()
            tmpfile = os.path.join(ARGS.volume_dir, uuid.uuid4().hex)
            with open(tmpfile, 'wb') as fd:
                for k in sorted(logs.keys()):
                    fd.write(logs[k][0])  # header
                    fd.write(logs[k][1])  # block
                    fd.write(logs[k][2])  # checksum

                    deletes.append([k])
                    inserts.append([k, log_seq_num, i, len(logs[k][1])])

                    i += len(logs[k][1]) + 64

            os.rename(tmpfile, os.path.join(logdir, str(log_seq_num)))

            db.executemany('delete from blocks where block=?', deletes)
            db.executemany('''insert into blocks
                              (block,lsn,offset,length)
                              values(?,?,?,?)
                           ''', inserts)
            db.commit()

            log('lsn(%d) read(%d) write(%d) discard(%d) msec(%d)',
                log_seq_num, stats['read'], stats['write'], stats['discard'],
                (time.time()-stats['ts'])*1000)

            logs = dict()
            stats = dict(ts=time.time(), read=0, write=0, discard=0)

        # send response to write, flush and discard command
        if cmd in (1, 3, 4):
            conn.sendall(response_header)

        # log('cmd(%s) offset(%d) count(%d) msec(%d)',
        #     cmd, block_offset, block_count, (time.time()-ts)*1000)

        # 0:read, 1:write, 3:flush, 4:discard
        if cmd not in (0, 1, 3, 4):
            panic('unsupported command')


def download_lsnfile(s3, lsn):
    lsnfile = os.path.join(ARGS.volume_dir, 'log', str(lsn))

    if not os.path.isfile(lsnfile):
        octets = s3.get(f'log/{lsn}')
        if not octets:
            panic(f'invalid lsn({lsn})')

        tmpfile = os.path.join(ARGS.volume_dir, uuid.uuid4().hex)
        with open(tmpfile, 'wb') as fd:
            fd.write(octets)
        os.rename(tmpfile, lsnfile)


def main():
    s3 = S3(ARGS.s3endpoint, ARGS.s3bucket, ARGS.s3prefix)
    tail = json.loads(s3.get('tail.json').decode())
    config = json.loads(s3.get('config.json').decode())

    logdir = os.path.join(ARGS.volume_dir, 'log')
    os.makedirs(logdir, exist_ok=True)

    index_db = os.path.join(ARGS.volume_dir, 'index.sqlite3')
    if not os.path.isfile(index_db):
        tmpfile = os.path.join(ARGS.volume_dir, uuid.uuid4().hex)
        with open(tmpfile, 'wb') as fd:
            fd.write(lz4.block.decompress(s3.get('index.latest')))

        tmpdb = os.path.join(ARGS.volume_dir, uuid.uuid4().hex)
        db = sqlite3.connect(tmpdb)
        db.execute('''create table if not exists blocks(
                          block  unsigned int primary key,
                          lsn    unsigned int,
                          offset unsigned int,
                          length unsigned int)
                   ''')

        filesize = os.path.getsize(tmpfile)
        with open(tmpfile, 'rb') as fd:
            sha = hashlib.sha256()
            for i in range((filesize-40)//32):
                octets = fd.read(32)
                sha.update(octets)
                db.execute('''insert into blocks(block,lsn,offset,length)
                              values(?,?,?,?)
                           ''', struct.unpack('!QQQQ', octets))
            db.commit()

            octets = fd.read(8)
            sha.update(octets)
            max_lsn = struct.unpack('!Q', octets)[0]

            if sha.digest() != fd.read(32):
                panic('checksum mismatch')

        os.remove(tmpfile)
        db.close()
        os.rename(tmpdb, index_db)

    db = sqlite3.connect(index_db)

    block_size = config['block_size']
    block_count = config['block_count']
    log_seq_num = tail['lsn']

    max_lsn = db.execute('select max(lsn) from blocks').fetchone()[0]
    if max_lsn is None:
        max_lsn = 0

    log('log_seq_num(%d) max_lsn(%d)', log_seq_num, max_lsn)

    for lsn in range(max_lsn+1, log_seq_num+1):
        download_lsnfile(s3, lsn)
        with open(os.path.join(logdir, str(lsn)), 'rb') as fd:
            i = j = 0
            deletes = list()
            inserts = list()
            while True:
                hdr = fd.read(32)
                if not hdr:
                    break

                blk, logseq, usec, length = struct.unpack('!QQQQ', hdr)

                block = fd.read(length)
                chksum = fd.read(32)

                sha = hashlib.sha256(hdr)
                sha.update(block)

                if logseq != lsn or sha.digest() != chksum:
                    panic('corrupt logfile(%d)', lsn)

                deletes.append([blk])
                inserts.append([blk, logseq, i, length])

                i += 32 + length + 32
                j += 1

            db.executemany('delete from blocks where block=?', deletes)
            db.executemany('''insert into blocks (block,lsn,offset,length)
                              values(?,?,?,?)
                           ''', inserts)
            db.commit()
            log('updated map(%d) blocks(%d)', lsn, j)

    if log_seq_num > max_lsn:
        sha = hashlib.sha256()
        lsn_set = list()
        tmpfile = os.path.join(ARGS.volume_dir, uuid.uuid4().hex)
        with open(tmpfile, 'wb') as fd:
            rows = db.execute('''select block,lsn,offset,length
                                 from blocks order by block''')
            for blk, lsn, offset, length in rows:
                octets = struct.pack('!QQQQ', blk, lsn, offset, length)
                fd.write(octets)
                sha.update(octets)
                lsn_set.append(lsn)

            lsn_set = set(lsn_set)
            octets = struct.pack('!Q', max(lsn_set) if lsn_set else 0)
            fd.write(octets)
            sha.update(octets)
            fd.write(sha.digest())

        with open(tmpfile, 'rb') as fd:
            s3.put('index.latest', lz4.block.compress(fd.read()))
        os.remove(tmpfile)
        log('uploaded new index lsn(%d)', max(lsn_set))

    db.close()

    # Start the backup thread
    args = (log_seq_num, logdir)
    threading.Thread(target=backup, args=args).start()

    # Initialize the unix domain server socket
    sock_path = os.path.join('/tmp', str(uuid.uuid4()))
    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_sock.bind(sock_path)
    server_sock.listen(1)
    log('server listening on sock(%s)', sock_path)

    # Start the server thread
    args = (server_sock, block_size, block_count,
            logdir, log_seq_num, index_db)
    threading.Thread(target=server, args=args).start()

    # Initialize the client socket, to be attached to the nbd device
    client_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client_sock.connect(sock_path)
    os.remove(sock_path)

    # Initialize the device, attach the client socket created above
    device_init(ARGS.device, block_size, block_count, client_sock.fileno())


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()

    ARGS.add_argument('--s3endpoint',
                      default='https://s3.us-east-005.backblazeb2.com',
                      help='S3 object store endpoint')

    ARGS.add_argument('--s3bucket', default='cloudblockdevice',
                      help='S3 bucket')

    ARGS.add_argument('--s3prefix', default='volume',
                      help='S3 prefix for namespace')

    ARGS.add_argument('--device', default='/dev/nbd0',
                      help='Network Block Device path')

    ARGS.add_argument('--volume_dir', default='volume',
                      help='volume write area')

    ARGS.add_argument('--block_size', type=int, help='Device block size')
    ARGS.add_argument('--block_count', type=int, help='Device block count')

    ARGS = ARGS.parse_args()

    if ARGS.block_size is None and ARGS.block_count is None:
        main()
    else:
        s3 = S3(ARGS.s3endpoint, ARGS.s3bucket, ARGS.s3prefix)
        s3.put('config.json', json.dumps(dict(
            block_size=ARGS.block_size,
            block_count=ARGS.block_count)).encode())
        s3.put('tail.json', json.dumps(dict(lsn=0)).encode())

        octets = struct.pack('!Q', 0)
        sha = hashlib.sha256(octets).digest()
        s3.put('index.latest', lz4.block.compress(octets + sha))

        log('Device initialized')
        log(json.loads(s3.get('tail.json').decode()))
        log(json.loads(s3.get('config.json').decode()))
        log(len(s3.get('index.latest')))
