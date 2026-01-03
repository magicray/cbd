import os
import time
import gzip
import uuid
import json
import boto3
import fcntl
import struct
import socket
import logging
import hashlib
import argparse
import threading
from logging import critical as log


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
    fcntl.ioctl(fd, NBD_SET_TIMEOUT, 30)
    fcntl.ioctl(fd, NBD_PRINT_DEBUG)
    fcntl.ioctl(fd, NBD_SET_SOCK, conn)

    # set FLUSH option
    fcntl.ioctl(fd, NBD_SET_FLAGS, 5)

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
    while length:
        octets = conn.recv(length)

        if not octets:
            conn.close()
            raise Exception('connection closed')

        buf.append(octets)
        length -= len(octets)

    return b''.join(buf)


def server(sock, block_size, device_block_count, logdir, log_seq_num, map_fd):
    s3 = S3(ARGS.s3endpoint, ARGS.s3bucket, ARGS.s3prefix)

    conn, peer = sock.accept()
    log('client connection accepted')

    zero_block = b'\x00' * block_size

    commands = ['read', 'write', None, 'flush']

    logs = dict()
    logts = time.time()

    while True:
        magic, flags, cmd, cookie, offset, length = struct.unpack(
            '!IHHQQI', recvall(conn, 28))

        ts = time.time()

        if 0x25609513 != magic:
            log(f'invalid magic({magic}) or cmd({cmd})')
            os._exit(1)

        if 0 != offset % block_size or 0 != length % block_size:
            log(f'invalid offset({offset}) or length({length})')
            os._exit(1)

        # Response header is common. No errors are supported.
        response_header = struct.pack('!IIQ', 0x67446698, 0, cookie)

        block_count = length // block_size
        block_offset = offset // block_size

        # READ
        if 0 == cmd:
            blocks = list()

            for i in range(block_count):
                j = block_offset + i

                block = zero_block

                if j in logs:
                    block = gzip.decompress(logs[j][1])
                else:
                    os.lseek(map_fd, j*16, os.SEEK_SET)
                    lsn, index = struct.unpack('!QQ', os.read(map_fd, 16))

                    if lsn:
                        download_lsnfile(s3, lsn)

                    lsn_file = os.path.join(logdir, str(lsn))
                    if os.path.isfile(lsn_file):
                        with open(lsn_file, 'rb') as fd:
                            fd.seek(index)

                            # header : block_index, lsn, usec_timestamp, length
                            hdr = fd.read(32)

                            # extract block_index, lsn, usec_timestamp
                            num, logseq, usec, length = struct.unpack(
                                '!QQQQ', hdr)

                            # this is the actual data
                            block = fd.read(length)

                            # checksum - sha256
                            chksum = fd.read(32)

                        if num != j or lsn != logseq:
                            log(('corrupt block', j, num, lsn, logseq, usec))
                            os._exit(0)

                        sha = hashlib.sha256(hdr)
                        sha.update(block)
                        if sha.digest() != chksum:
                            log(block)
                            log(('corrupt block', num, lsn, usec, chksum))
                            os._exit(0)

                        block = gzip.decompress(block)

                blocks.append(block)

            conn.sendall(response_header)
            conn.sendall(b''.join(blocks))

        # WRITE
        if 1 == cmd:
            octets = recvall(conn, length)

            for i in range(block_count):
                block = octets[i*block_size:(i+1)*block_size]
                compressed = gzip.compress(block, compresslevel=1)

                hdr = struct.pack('!QQQQ', block_offset+i, log_seq_num+1,
                                  int(time.time()*1000000), len(compressed))

                sha = hashlib.sha256(hdr)
                sha.update(compressed)

                logs[block_offset+i] = [hdr, compressed, sha.digest()]

            conn.sendall(response_header)

        # FLUSH
        if 3 == cmd or time.time() - logts > 10:
            if logs:
                log_seq_num += 1

                i = 0
                tmpfile = os.path.join(ARGS.volume_dir, uuid.uuid4().hex)
                with open(tmpfile, 'wb') as fd:
                    for k in sorted(logs.keys()):
                        fd.write(logs[k][0])  # header
                        fd.write(logs[k][1])  # block
                        fd.write(logs[k][2])  # checksum

                        # map : lsn+offset (8+4 bytes)
                        os.lseek(map_fd, k*16, os.SEEK_SET)
                        os.write(map_fd, struct.pack('!QQ', log_seq_num, i))

                        i += len(logs[k][1]) + 64

                os.rename(tmpfile, os.path.join(logdir, str(log_seq_num)))

                # sync map file to disk
                os.fsync(map_fd)
                os.lseek(map_fd, device_block_count*16, os.SEEK_SET)
                os.write(map_fd, struct.pack('!Q', log_seq_num))

                log('lsn(%d) blocks(%d) msec(%d)',
                    log_seq_num, len(logs), (time.time()-ts)*1000)

                logs = dict()

            # send response to FLUSH command
            if 3 == cmd:
                conn.sendall(response_header)

            logts = time.time()

        log('cmd(%s) offset(%d) count(%d) msec(%d)',
            commands[cmd], block_offset, block_count, (time.time()-ts)*1000)

        if cmd not in (0, 1, 3):
            log('unsupported command')
            os._exit(1)


def download_lsnfile(s3, lsn):
    lsnfile = os.path.join(ARGS.volume_dir, 'log', str(lsn))

    if not os.path.isfile(lsnfile):
        octets = s3.get(f'log/{lsn}')
        if not octets:
            log(f'invalid lsn({lsn})')
            os._exit(1)

        tmpfile = os.path.join(ARGS.volume_dir, uuid.uuid4().hex)
        with open(tmpfile, 'wb') as fd:
            fd.write(octets)
        os.rename(tmpfile, lsnfile)


def main():
    s3 = S3(ARGS.s3endpoint, ARGS.s3bucket, ARGS.s3prefix)
    tail = json.loads(s3.get('tail.json').decode())
    config = json.loads(s3.get('config.json').decode())

    block_size = config['block_size']
    block_count = config['block_count']
    log_seq_num = tail['lsn']

    logdir = os.path.join(ARGS.volume_dir, 'log')
    os.makedirs(logdir, exist_ok=True)

    block_map = os.path.join(ARGS.volume_dir, 'block_map')
    map_fd = os.open(block_map, os.O_CREAT | os.O_RDWR)
    os.lseek(map_fd, block_count*16 + 8, os.SEEK_SET)
    os.write(map_fd, b'MAP')

    os.lseek(map_fd, block_count*16, os.SEEK_SET)
    map_lsn = struct.unpack('!Q', os.read(map_fd, 8))[0]
    log('log_seq_num(%d) map_lsn(%d)', log_seq_num, map_lsn)

    for lsn in range(map_lsn+1, log_seq_num+1):
        download_lsnfile(s3, lsn)
        with open(os.path.join(logdir, str(lsn)), 'rb') as fd:
            i = 0
            j = 0
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
                    log('corrupt logfile(%d)', lsn)
                    os._exit(1)

                os.lseek(map_fd, blk*16, os.SEEK_SET)
                os.write(map_fd, struct.pack('!QQ', logseq, i))
                i += length + 64
                j += 1

            log('updated map(%d) blocks(%d)', lsn, j)

        os.lseek(map_fd, block_count * 16, os.SEEK_SET)
        os.write(map_fd, struct.pack('!Q', lsn))
        os.fsync(map_fd)

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
            logdir, log_seq_num, map_fd)
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

        log('Device initialized')
        log(json.loads(s3.get('tail.json').decode()))
        log(json.loads(s3.get('config.json').decode()))
