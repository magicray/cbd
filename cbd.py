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
    # NBD_SET_FLAGS = 43786

    fd = os.open(dev, os.O_RDWR)
    fcntl.ioctl(fd, NBD_CLEAR_QUEUE)
    fcntl.ioctl(fd, NBD_DISCONNECT)
    fcntl.ioctl(fd, NBD_CLEAR_SOCK)
    fcntl.ioctl(fd, NBD_SET_BLKSIZE, block_size)
    fcntl.ioctl(fd, NBD_SET_SIZE_BLOCKS, block_count)
    fcntl.ioctl(fd, NBD_SET_TIMEOUT, 30)
    fcntl.ioctl(fd, NBD_PRINT_DEBUG)
    fcntl.ioctl(fd, NBD_SET_SOCK, conn)

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


def backup(batch, batch_lock):
    while True:
        logbytes = list()
        with batch_lock:
            for k, v in batch['frozen'].items():
                logbytes.append(struct.pack('!Q', k))
                logbytes.append(v)
                logbytes.append(hashlib.sha256(v).digest())

        block_count = len(logbytes) // 3
        logbytes = b''.join(logbytes)
        compressed = gzip.compress(logbytes)

        if logbytes:
            with open(os.path.join(ARGS.volume_dir, 'tmp'), 'wb') as fd:
                fd.write(compressed)

            os.rename(os.path.join(ARGS.volume_dir, 'tmp'),
                      os.path.join(ARGS.volume_dir, 'txn'))

        # Write the blocks in the frozen batch to local cache
        fd = os.open(os.path.join(ARGS.volume_dir, 'cache.active'),
                     os.O_CREAT | os.O_WRONLY)
        os.lseek(fd, (ARGS.block_size+40) * ARGS.block_count, os.SEEK_SET)
        os.write(fd, b'CBD')

        i = 0
        while i < len(logbytes):
            buf = logbytes[i:i+ARGS.block_size+40]

            k = struct.unpack('!Q', buf[:8])[0]
            os.lseek(fd, k*(ARGS.block_size+40), os.SEEK_SET)
            os.write(fd, buf)

            i += ARGS.block_size + 40

        os.close(fd)

        # We are done with the frozen batch. Discard it now.
        # Mark the current active batch as frozen and create a new
        # empty active batch
        with batch_lock:
            batch['frozen'] = batch['active']
            batch['active'] = dict()

        if logbytes:
            log('blocks({}) bytes({}) compressed({})'.format(
                block_count, len(logbytes), len(compressed)))

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


def server(sock, batch, batch_lock):
    conn, peer = sock.accept()
    log('client connection accepted')

    zerobuf = b'\x00' * 32

    active_fd = frozen_fd = None
    active_cache = os.path.join(ARGS.volume_dir, 'cache.active')
    frozen_cache = os.path.join(ARGS.volume_dir, 'cache.frozen')

    with batch_lock:
        txn = os.path.join(ARGS.volume_dir, 'txn')
        if os.path.isfile(txn):
            with open(txn, 'rb') as fd:
                octets = gzip.decompress(fd.read())

            i = 0
            while i < len(octets):
                buf = octets[i:i+ARGS.block_size+40]
                num = struct.unpack('!Q', buf[:8])[0]
                batch['active'][num] = buf[8:-32]

                i += ARGS.block_size+40

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

        if active_fd:
            if os.fstat(active_fd).st_blocks * 512 > ARGS.cache_file_size:
                if frozen_fd is None:
                    os.rename(active_cache, frozen_cache)
                    frozen_fd = active_fd
                    active_fd = None

        if active_fd is None and os.path.isfile(active_cache):
            active_fd = os.open(active_cache, os.O_RDONLY)

        if frozen_fd is None and os.path.isfile(frozen_cache):
            frozen_fd = os.open(frozen_cache, os.O_RDONLY)

        # READ
        if 0 == cmd:
            blocks = list()

            for i in range(block_count):
                j = block_offset + i
                block = b''

                with batch_lock:
                    if j in batch['active']:
                        block = batch['active'][j]
                    elif j in batch['frozen']:
                        block = batch['frozen'][j]

                if not block:
                    for fd in (active_fd, frozen_fd):
                        if fd is not None:
                            os.lseek(fd, j*(ARGS.block_size+40), os.SEEK_SET)

                            num = struct.unpack('!Q', os.read(fd, 8))[0]
                            block = os.read(fd, ARGS.block_size)
                            chksum = os.read(fd, 32)

                            if chksum != zerobuf:
                                break

                    if num not in (0, j):
                        log((num, j))
                        log('corruption detected')
                        os._exit(0)

                    if chksum != zerobuf:
                        if hashlib.sha256(block).digest() != chksum:
                            log(num)
                            log(block)
                            log(chksum)
                            log((j, offset, length))
                            log('corruption detected')
                            os._exit(0)

                blocks.append(block)

            conn.sendall(response_header)
            conn.sendall(b''.join(blocks))

            log('read offset(%d) length(%d) msec(%d)',
                offset, length, (time.time()-ts)*1000)

        # WRITE
        elif 1 == cmd:
            octets = recvall(conn, length)

            with batch_lock:
                for i in range(block_count):
                    block = octets[i*ARGS.block_size:(i+1)*ARGS.block_size]
                    batch['active'][block_offset+i] = block

            conn.sendall(response_header)

            if len(batch['active']) > ARGS.max_active_block_count:
                time.sleep(0.1)

            log('write offset(%d) length(%d) msec(%d)',
                offset, length, (time.time()-ts)*1000)
        else:
            log('cmd(%d) offset(%d) length(%d) msec(%d)',
                cmd, offset, length, (time.time()-ts)*1000)
            os._exit(1)


def main():
    batch = dict(active=dict(), frozen=dict())
    batch_lock = threading.Lock()

    os.makedirs(os.path.join(ARGS.volume_dir), exist_ok=True)
    """
    s3 = S3(ARGS.prefix,
            os.environ['CBD_S3_ENDPOINT'], os.environ['CBD_S3_BUCKET'],
            os.environ['CBD_S3_AUTH_KEY'], os.environ['CBD_S3_AUTH_SECRET'])
    """

    # Start the backup thread
    args = (batch, batch_lock)
    threading.Thread(target=backup, args=args).start()

    # Initialize the unix domain server socket
    sock_path = os.path.join('/tmp', str(uuid.uuid4()))
    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_sock.bind(sock_path)
    server_sock.listen(1)
    log('server listening on sock(%s)', sock_path)

    # Start the server thread
    args = (server_sock, batch, batch_lock)
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

    ARGS.add_argument('--max_active_block_count', type=int, default=10000,
                      help='Maximum active blocks, for rate limiting')

    ARGS.add_argument('--volume_dir', default='volume',
                      help='volume write area')

    ARGS.add_argument('--cache_file_size', default=1024*1024*1024,
                      help='maximum cache file size')

    ARGS = ARGS.parse_args()

    main()
