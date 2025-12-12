import os
import time
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
        # Work on the frozen batch now
        for k in batch['frozen']:
            block = batch['frozen'][k]['block']
            sha256 = hashlib.sha256(block).digest()
            blob_io(k*ARGS.block_size, block, sha256)

        with batch_lock:
            batch['frozen'] = batch['active']
            batch['active'] = dict()

        time.sleep(1)

    while True:
        # Upload the octets to log and log_index to details.json
        # s3.create('logs/{}'.format(1), body)
        pass


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


blob_fd_cache = dict()


def blob_io(offset, block=None, sha256=None):
    blob_size = ARGS.blocks_per_blob * ARGS.block_size

    blob_num = (offset // blob_size) * blob_size

    blob_path = os.path.join(ARGS.volume_dir, 'objs', str(blob_num))

    if blob_path not in blob_fd_cache:
        fd = os.open(blob_path, os.O_CREAT | os.O_RDWR)
        blob_fd_cache[blob_path] = fd

    fd = blob_fd_cache[blob_path]

    block_index = (offset - blob_num) // ARGS.block_size
    blob_offset = block_index * (ARGS.block_size + 32)
    os.lseek(fd, blob_offset, os.SEEK_SET)

    if block:
        os.write(fd, block + sha256)
        os.fsync(fd)
    else:
        block = os.read(fd, ARGS.block_size + 32)

        if not block:
            return b'\0' * ARGS.block_size

        if hashlib.sha256(block[:-32]).digest() != block[-32:]:
            if block != b'\0' * len(block):
                log('not a zero filled block')
                os._exit(1)

        return block[:ARGS.block_size]


def server(sock, batch, batch_lock):
    conn, peer = sock.accept()
    log('client connection accepted')

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
                block = b''

                with batch_lock:
                    if j in batch['active']:
                        block = batch['active'][block_offset+i]['block']
                    elif j in batch['frozen']:
                        block = batch['frozen'][block_offset+i]['block']

                if not block:
                    block = blob_io(j*ARGS.block_size)

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
                    batch['active'][block_offset+i] = dict(block=block)

            conn.sendall(response_header)

            log('write offset(%d) length(%d) msec(%d)',
                offset, length, (time.time()-ts)*1000)
        else:
            log('cmd(%d) offset(%d) length(%d) msec(%d)',
                cmd, offset, length, (time.time()-ts)*1000)
            os._exit(0)


def main():
    batch = dict(active=dict(), frozen=dict())
    batch_lock = threading.Lock()
    """
    s3 = S3(ARGS.prefix,
            os.environ['CBD_S3_ENDPOINT'], os.environ['CBD_S3_BUCKET'],
            os.environ['CBD_S3_AUTH_KEY'], os.environ['CBD_S3_AUTH_SECRET'])
    """

    os.makedirs(os.path.join(ARGS.volume_dir, 'objs'), exist_ok=True)

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

    ARGS.add_argument('--block_count', type=int, default=256*1024,
                      help='Device Block Count')

    ARGS.add_argument('--volume_dir', default='volume',
                      help='volume write area')

    ARGS.add_argument('--blocks_per_blob', default=10000,
                      help='blocks per blob')

    ARGS = ARGS.parse_args()

    main()
