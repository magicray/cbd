import os
import time
import gzip
import uuid
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
    def __init__(self, prefix, endpoint, bucket, key_id, secret_key):
        self.prefix = prefix
        self.bucket = bucket
        self.endpoint = endpoint

        self.s3 = boto3.client('s3', endpoint_url=self.endpoint,
                               aws_access_key_id=key_id,
                               aws_secret_access_key=secret_key)

        self.snapshot_min = self.log_min = 2**64
        self.snapshot_max = self.log_max = -1
        self.size = 0

        res = self.s3.list_objects(Bucket=self.bucket, Prefix=prefix)
        for obj in res.get('Contents', []):
            key, size = obj['Key'], obj['Size']

            tmp = key.split('/')
            if tmp[-2] == 'logs':
                self.log_min = min(self.log_min, int(tmp[-1]))
                self.log_max = max(self.log_max, int(tmp[-1]))
            elif tmp[-2] == 'snapshots':
                self.snapshot_min = min(self.snapshot_min, int(tmp[-1]))
                self.snapshot_max = max(self.snapshot_max, int(tmp[-1]))

            self.size += size

        log('bucket(%s/%s) prefix(%s) log(%d, %d) snapshot(%d, %d) size(%d)',
            endpoint, bucket, prefix,
            self.log_min, self.log_max,
            self.snapshot_min, self.snapshot_max,
            self.size)

    def create(self, key, value):
        ts = time.time()
        key = os.path.join(self.prefix, key)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=value,
                           IfNoneMatch='*')
        log('bucket(%s/%s) create(%s) length(%d) msec(%d)',
            self.endpoint, self.bucket, key, len(value),
            (time.time()-ts) * 1000)

    def read(self, key):
        ts = time.time()
        key = os.path.join(self.prefix, key)

        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            octets = obj['Body'].read()
            assert (len(octets) == obj['ContentLength'])
        except self.s3.exceptions.NoSuchKey:
            octets = ''

        log('bucket(%s/%s) read(%s) length(%d) msec(%d)',
            self.endpoint, self.bucket, key, len(octets),
            (time.time()-ts) * 1000)

        return octets if octets else None


def backup():
    while True:
        time.sleep(1)


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


def server(sock, logdir, log_seq_num, map_fd):
    conn, peer = sock.accept()
    log('client connection accepted')

    zero_block = b'\x00' * ARGS.block_size

    commands = ['read', 'write', None, 'flush']

    # block + lsn + usec_timestamp + sha256
    header_size = 8 + 8 + 8 + 32
    header_block_size = ARGS.block_size + header_size

    logs = dict()
    logts = time.time()

    while True:
        magic, flags, cmd, cookie, offset, length = struct.unpack(
            '!IHHQQI', recvall(conn, 28))

        ts = time.time()

        if 0x25609513 != magic:
            log(f'invalid magic({magic}) or cmd({cmd})')
            os._exit(1)

        if 0 != offset % ARGS.block_size or 0 != length % ARGS.block_size:
            log(f'invalid offset({offset}) or length({length})')
            os._exit(1)

        # Response header is common. No errors are supported.
        response_header = struct.pack('!IIQ', 0x67446698, 0, cookie)

        block_count = length // ARGS.block_size
        block_offset = offset // ARGS.block_size

        # READ
        if 0 == cmd:
            blocks = list()

            for i in range(block_count):
                j = block_offset + i

                block = zero_block

                if j in logs:
                    block = logs[j][1]
                else:
                    os.lseek(map_fd, j*12, os.SEEK_SET)
                    lsn, index = struct.unpack('!QI', os.read(map_fd, 12))

                    if os.path.isfile(os.path.join(logdir, str(lsn))):
                        with open(os.path.join(logdir, str(lsn)), 'rb') as fd:
                            fd.seek(index * header_block_size)
                            hdr = fd.read(24)
                            block = fd.read(ARGS.block_size)
                            chksum = fd.read(32)

                        num, logseq, usec = struct.unpack('!QQQ', hdr)

                        if num != j or lsn != logseq:
                            log(('corrupt block', j, num, lsn, logseq, usec))
                            os._exit(0)

                        sha = hashlib.sha256(hdr)
                        sha.update(block)
                        if sha.digest() != chksum:
                            log(block)
                            log(('corrupt block', j, num, offset, length,
                                 chksum))
                            os._exit(0)

                blocks.append(block)

            conn.sendall(response_header)
            conn.sendall(b''.join(blocks))

        # WRITE
        if 1 == cmd:
            octets = recvall(conn, length)

            for i in range(block_count):
                block = octets[i*ARGS.block_size:(i+1)*ARGS.block_size]

                usec = int(time.time()*1000000)
                hdr = struct.pack('!QQQ', block_offset+i, log_seq_num+1, usec)

                sha = hashlib.sha256(hdr)
                sha.update(block)

                logs[block_offset+i] = [hdr, block, sha.digest()]

            conn.sendall(response_header)

        # FLUSH
        if 3 == cmd or time.time() - logts > 1:
            if logs:
                log_seq_num += 1

                tmpfile = os.path.join(logdir, uuid.uuid4().hex)
                with open(tmpfile, 'wb') as fd:
                    for i, k in enumerate(sorted(logs.keys())):
                        fd.write(logs[k][0])
                        fd.write(logs[k][1])
                        fd.write(logs[k][2])

                        os.lseek(map_fd, k*12, os.SEEK_SET)
                        os.write(map_fd, struct.pack('!QI', log_seq_num, i))

                os.rename(tmpfile, os.path.join(logdir, str(log_seq_num)))

                os.fsync(map_fd)
                os.lseek(map_fd, ARGS.block_count*12, os.SEEK_SET)
                os.write(map_fd, struct.pack('!Q', log_seq_num))
                os.fsync(map_fd)

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


def main():
    logdir = os.path.join(ARGS.volume_dir, 'logs')
    os.makedirs(logdir, exist_ok=True)

    logfiles = [int(f) for f in os.listdir(logdir)]
    log_seq_num = max(logfiles) if logfiles else 0

    block_map = os.path.join(ARGS.volume_dir, 'block_map')
    map_fd = os.open(block_map, os.O_CREAT | os.O_RDWR)
    os.lseek(map_fd, ARGS.block_count * 12 + 8, os.SEEK_SET)
    os.write(map_fd, b'MAP')

    os.lseek(map_fd, ARGS.block_count * 12, os.SEEK_SET)
    map_lsn = struct.unpack('!Q', os.read(map_fd, 8))[0]
    log('log_lsn(%d) map_lsn(%d)', log_seq_num, map_lsn)
    if log_seq_num != map_lsn:
        os._exit(1)

    """
    s3 = S3(ARGS.prefix,
            os.environ['CBD_S3_ENDPOINT'], os.environ['CBD_S3_BUCKET'],
            os.environ['CBD_S3_AUTH_KEY'], os.environ['CBD_S3_AUTH_SECRET'])
    """

    # Start the backup thread
    threading.Thread(target=backup).start()

    # Initialize the unix domain server socket
    sock_path = os.path.join('/tmp', str(uuid.uuid4()))
    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_sock.bind(sock_path)
    server_sock.listen(1)
    log('server listening on sock(%s)', sock_path)

    # Start the server thread
    args = (server_sock, logdir, log_seq_num, map_fd)
    threading.Thread(target=server, args=args).start()

    # Initialize the client socket, to be attached to the nbd device
    client_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client_sock.connect(sock_path)
    os.remove(sock_path)

    # Initialize the device, attach the client socket created above
    device_init(ARGS.device, ARGS.block_size, ARGS.block_count,
                client_sock.fileno())


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()

    ARGS.add_argument('--device', default='/dev/nbd0',
                      help='Network Block Device path')

    ARGS.add_argument('--block_size', type=int, default=4096,
                      help='Device Block Size')

    ARGS.add_argument('--block_count', type=int, default=25*1024*1024,
                      help='Device Block Count')

    ARGS.add_argument('--volume_dir', default='volume',
                      help='volume write area')

    ARGS = ARGS.parse_args()

    main()
