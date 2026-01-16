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


BLOCK_SIZE = 4096
BLOCK_COUNT = 2**31-1


def panic(msg):
    log(msg)
    traceback.print_exc()
    os._exit(1)


def device_init(dev, conn):
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
    fcntl.ioctl(fd, NBD_SET_BLKSIZE, BLOCK_SIZE)
    fcntl.ioctl(fd, NBD_SET_SIZE_BLOCKS, BLOCK_COUNT)
    fcntl.ioctl(fd, NBD_SET_TIMEOUT, 60)
    fcntl.ioctl(fd, NBD_PRINT_DEBUG)
    fcntl.ioctl(fd, NBD_SET_SOCK, conn)

    # set options : FLUSH(4)
    fcntl.ioctl(fd, NBD_SET_FLAGS, 4+1)

    log('initialized(%s)', dev)

    # Block forever
    fcntl.ioctl(fd, NBD_DO_IT)


class S3:
    def __init__(self, bucket, prefix):
        self.prefix = prefix
        self.bucket = bucket.split('/')[-1]
        self.endpoint = '/'.join(bucket.split('/')[:-1])

        if self.endpoint:
            self.s3 = boto3.client('s3', endpoint_url=self.endpoint)
        else:
            for d in ('log', 'index'):
                os.makedirs(os.path.join(bucket, prefix, d), exist_ok=True)

    def put(self, key, value):
        ts = time.time()
        key = os.path.join(self.prefix, key)

        if ARGS.lsn is None:
            if self.endpoint:
                self.s3.put_object(Bucket=self.bucket, Key=key, Body=value)
            else:
                tmpfile = os.path.join(self.bucket, uuid.uuid4().hex)
                with open(tmpfile, 'wb') as fd:
                    fd.write(value)
                os.rename(tmpfile, os.path.join(self.bucket, key))

            log('put(%s/%s/%s) length(%d) msec(%d)',
                self.endpoint, self.bucket, key, len(value),
                (time.time()-ts) * 1000)
        else:
            log('skipped put(%s/%s/%s) length(%d) msec(%d)',
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

        log('get(%s/%s/%s) length(%d) msec(%d)',
            self.endpoint, self.bucket, key, len(octets),
            (time.time()-ts) * 1000)

        return octets if octets else None


def backup(remote_lsn, logdir):
    s3 = S3(ARGS.bucket, ARGS.prefix)

    while True:
        try:
            lsnfile = os.path.join(logdir, str(remote_lsn+1))
            if os.path.isfile(lsnfile):
                with open(lsnfile, 'rb') as fd:
                    s3.put(f'log/{remote_lsn+1}', fd.read())

                s3.put('log.json', json.dumps(dict(lsn=remote_lsn+1)).encode())

                remote_lsn += 1
            else:
                time.sleep(1)

        except Exception:
            traceback.print_exc()
            time.sleep(10)


def recvall(conn, length):
    buf = list()

    remaining = length
    while remaining > 0:
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


def server(sock, logdir, remote_lsn, db):
    db = sqlite3.connect(db)
    s3 = S3(ARGS.bucket, ARGS.prefix)

    conn, peer = sock.accept()
    log('client connection accepted')

    zeroed_block = lz4.block.compress(bytearray(BLOCK_SIZE))

    fds = dict()
    logs = dict()

    stats = dict(ts=time.time(), read=0, write=0)

    while True:
        magic, flags, cmd, cookie, req_offset, req_length = struct.unpack(
            '!IHHQQI', recvall(conn, 28))

        ts = time.time()

        if 0x25609513 != magic:
            panic(f'invalid magic({magic}) or cmd({cmd})')

        if 0 != req_offset % BLOCK_SIZE or 0 != req_length % BLOCK_SIZE:
            panic(f'invalid offset({req_offset}) or length({req_length})')

        # Response header is common. No errors are supported.
        response_header = struct.pack('!IIQ', 0x67446698, 0, cookie)

        block_count = req_length // BLOCK_SIZE
        block_offset = req_offset // BLOCK_SIZE

        # READ
        if 0 == cmd:
            blocks = list()

            # read log file number and offset for block_count blocks
            rows = db.execute('''select block, lsn, offset, length from blocks
                                 where block between ? and ?
                              ''', [block_offset, block_offset+block_count-1])
            if rows:
                # key -> block number
                # value -> log file number, offset into log file, length
                rows = {r[0]: (r[1], r[2], r[3]) for r in rows.fetchall()}

            for i in range(block_offset, block_offset+block_count):
                # latest value is not yet written to the log
                if i in logs:
                    block = logs[i][1]

                # this block found in some log file
                elif i in rows:
                    lsn, index, length = rows[i]

                    # if already present, this function would do nothing
                    download_lsnfile(s3, lsn)

                    lsn_file = os.path.join(logdir, str(lsn))
                    if lsn_file not in fds:
                        fds[lsn_file] = os.open(lsn_file, os.O_RDONLY)

                    octets = os.pread(fds[lsn_file], 32+length+32, index)

                    hdr = octets[:32]       # header
                    block = octets[32:-32]  # actual block content - compressed
                    chksum = octets[-32:]   # checksum - sha256

                    # header : block_index, lsn, usec_timestamp, length
                    num, logseq, usec, length = struct.unpack('!QQQQ', hdr)

                    if num != i or lsn != logseq:
                        panic(('corrupt block', i, num, lsn, logseq, usec))

                    sha = hashlib.sha256(hdr)
                    sha.update(block)
                    if sha.digest() != chksum:
                        log(block)
                        panic(('corrupt block', num, lsn, usec, chksum))

                # this block was never written
                else:
                    block = zeroed_block

                blocks.append(lz4.block.decompress(block))

            # concatenate all the block to get the response data
            octets = b''.join(blocks)

            if len(octets) != req_length:
                panic(f'read length({req_length}) mismatch({len(octets)})')

            stats['read'] += block_count
            conn.sendall(response_header)
            conn.sendall(octets)

        # WRITE
        if 1 == cmd:
            for i in range(block_offset, block_offset+block_count):
                # read the block from the socket and compress it
                octets = lz4.block.compress(recvall(conn, BLOCK_SIZE))

                # header : block_index, lsn, usec_timestamp, length
                hdr = struct.pack('!QQQQ', i, remote_lsn+1,
                                  int(ts*1000000), len(octets))

                sha = hashlib.sha256(hdr)
                sha.update(octets)

                # retain data in memory till it is written to the log
                logs[i] = [hdr, octets, sha.digest()]

            stats['write'] += block_count

        # FLUSH
        if (3 == cmd and len(logs) > 0) or len(logs) > 1024:
            remote_lsn += 1

            deletes = list()
            inserts = list()
            tmpfile = os.path.join(ARGS.datadir, ARGS.prefix, uuid.uuid4().hex)
            with open(tmpfile, 'wb') as fd:
                for k in sorted(logs.keys()):
                    offset = fd.tell()

                    fd.write(logs[k][0])  # header   - 32 bytes
                    fd.write(logs[k][1])  # block    - compressed block_size
                    fd.write(logs[k][2])  # checksum - 32 bytes

                    # delete the existing row and insert the new value
                    deletes.append([k])
                    inserts.append([k, remote_lsn, offset, len(logs[k][1])])

                fd.write(struct.pack('!Q', fd.tell()+8))  # eof marker

            # atomically rename the tmp file now, avoiding half written files
            os.rename(tmpfile, os.path.join(logdir, str(remote_lsn)))

            # update the index database
            db.executemany('delete from blocks where block=?', deletes)
            db.executemany('''insert into blocks
                              (block,lsn,offset,length)
                              values(?,?,?,?)
                           ''', inserts)
            db.commit()

            log('lsn(%d) read(%d) write(%d) msec(%d)',
                remote_lsn, stats['read'], stats['write'],
                (time.time()-stats['ts'])*1000)

            logs = dict()
            stats = dict(ts=time.time(), read=0, write=0)

        # send response to write, flush and discard command
        if cmd in (1, 3):
            conn.sendall(response_header)

        # log('cmd(%s) offset(%d) count(%d) msec(%d)',
        #     cmd, block_offset, block_count, (time.time()-ts)*1000)

        # 0:read, 1:write, 3:flush
        if cmd not in (0, 1, 3):
            panic('unsupported command')


def download_lsnfile(s3, lsn):
    lsnfile = os.path.join(ARGS.datadir, ARGS.prefix, 'log', str(lsn))

    # download only if the file is not already downloaded
    if not os.path.isfile(lsnfile):
        octets = s3.get(f'log/{lsn}')
        if not octets:
            panic(f'empty log file({lsn})')

        # write data to a tmp file and then atomically rename
        tmpfile = os.path.join(ARGS.datadir, ARGS.prefix, uuid.uuid4().hex)
        with open(tmpfile, 'wb') as fd:
            fd.write(octets)
        os.rename(tmpfile, lsnfile)


def start():
    s3 = S3(ARGS.bucket, ARGS.prefix)

    if ARGS.lsn is None:
        remote_lsn = json.loads(s3.get('log.json').decode())['lsn']
    else:
        remote_lsn = ARGS.lsn

    logdir = os.path.join(ARGS.datadir, ARGS.prefix, 'log')
    index_db = os.path.join(ARGS.datadir, ARGS.prefix, 'index.sqlite3')
    os.makedirs(logdir, exist_ok=True)

    # download the index db if it is not already present
    if not os.path.isfile(index_db):
        index = json.loads(s3.get('index.json').decode())
        log('index : %s', sorted(map(int, index)))
        lsn = max([int(k) for k in index.keys() if int(k) <= remote_lsn])

        tmpfile = os.path.join(ARGS.datadir, ARGS.prefix, uuid.uuid4().hex)
        with open(tmpfile, 'wb') as fd:
            fd.write(lz4.block.decompress(s3.get(f'index/{lsn}')))
        os.rename(tmpfile, index_db)

    db = sqlite3.connect(index_db)

    local_lsn = db.execute('select max(lsn) from blocks').fetchone()[0]
    if local_lsn is None:
        local_lsn = 0

    log('remote_lsn(%d) local_lsn(%d)', remote_lsn, local_lsn)

    # update the index db with the latest log files
    for lsn in range(local_lsn+1, remote_lsn+1):
        download_lsnfile(s3, lsn)

        deletes = list()
        inserts = list()

        # read each record and verify checksum
        with open(os.path.join(logdir, str(lsn)), 'rb') as fd:
            while True:
                offset = fd.tell()

                hdr = fd.read(32)

                if 8 == len(hdr):
                    # reached end of the file
                    if fd.tell() != struct.unpack('!Q', hdr)[0]:
                        panic('corrupt file')
                    else:
                        break  # this file is successfully processed

                blk, logseq, usec, length = struct.unpack('!QQQQ', hdr)

                sha = hashlib.sha256(hdr)
                sha.update(fd.read(length))

                if logseq != lsn or sha.digest() != fd.read(32):
                    panic('corrupt logfile(%d)', lsn)

                deletes.append([blk])
                inserts.append([blk, logseq, offset, length])

        # all records are consisent, update the index
        db.executemany('delete from blocks where block=?', deletes)
        db.executemany('''insert into blocks (block,lsn,offset,length)
                          values(?,?,?,?)
                       ''', inserts)
        db.commit()
        log('updated map(%d) blocks(%d)', lsn, len(inserts))

    row = db.execute('''select count(distinct block), count(distinct lsn)
                        from blocks''').fetchone()
    log('total_blocks(%d) total_files(%d)', row[0], row[1])

    local_lsn = db.execute('select max(lsn) from blocks').fetchone()[0]
    if local_lsn is None:
        local_lsn = 0
    db.close()

    # upload the updated index db to the object store
    index = json.loads(s3.get('index.json').decode())
    if str(local_lsn) not in index and local_lsn <= remote_lsn:
        with open(index_db, 'rb') as fd:
            octets = lz4.block.compress(fd.read())
        s3.put(f'index/{local_lsn}', octets)

        index[local_lsn] = len(octets)
        s3.put('index.json', json.dumps(index).encode())
        log('uploaded new index lsn(%d)', local_lsn)

    return remote_lsn, logdir, index_db


def run(remote_lsn, logdir, index_db):
    # Start the backup thread
    args = (remote_lsn, logdir)
    threading.Thread(target=backup, args=args).start()

    # Initialize the unix domain server socket
    sock_path = os.path.join('/tmp', str(uuid.uuid4()))
    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_sock.bind(sock_path)
    server_sock.listen(1)
    log('server listening on sock(%s)', sock_path)

    # Start the server thread
    args = (server_sock, logdir, remote_lsn, index_db)
    threading.Thread(target=server, args=args).start()

    # Initialize the client socket, to be attached to the nbd device
    client_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client_sock.connect(sock_path)
    os.remove(sock_path)

    # Initialize the device, attach the client socket created above
    device_init(ARGS.device, client_sock.fileno())


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()

    ARGS.add_argument('--format_bucket', action='store_true',
                      help='initialize the object store')
    ARGS.add_argument('--update', action='store_true',
                      help='update the latest index')

    ARGS.add_argument('--device', help='device path')
    ARGS.add_argument('--lsn', type=int,
                      help='specific lsn to use for recovery')

    ARGS.add_argument('--prefix', default='prefix', help='file path prefix')
    ARGS.add_argument('--datadir', default='datadir', help='local write area')
    ARGS.add_argument(
        '--bucket',
        default='https://s3.us-east-005.backblazeb2.com/cloudblockdevice',
        help='object store')

    ARGS = ARGS.parse_args()

    if ARGS.update:
        start()

    elif ARGS.device:
        run(*start())

    elif ARGS.format_bucket:
        s3 = S3(ARGS.bucket, ARGS.prefix)
        s3.put('log.json', json.dumps(dict(lsn=0)).encode())

        tmpdb = os.path.join('/tmp', uuid.uuid4().hex)
        db = sqlite3.connect(tmpdb)
        db.execute('''create table if not exists blocks(
                          block  unsigned int primary key,
                          lsn    unsigned int,
                          offset unsigned int,
                          length unsigned int)
                   ''')
        db.close()

        with open(tmpdb, 'rb') as fd:
            octets = lz4.block.compress(fd.read())
            s3.put('index/0', octets)
        os.remove(tmpdb)
        s3.put('index.json', json.dumps({0: len(octets)}).encode())
        log('store initialized')

        log(json.loads(s3.get('log.json').decode()))
        log(len(s3.get('index/0')))
        log(json.loads(s3.get('index.json').decode()))
