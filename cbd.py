import os
import time
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


class Logger:
    def __init__(self, logdir):
        self.ts = 0
        self.fd = None
        self.logdir = logdir

    def append(self, meta, data):
        ts = int(time.time())

        if ts > self.ts:
            if self.fd:
                os.close(self.fd)

            self.ts = ts
            self.fd = os.open(os.path.join(self.logdir, str(self.ts)),
                              os.O_CREAT | os.O_WRONLY | os.O_APPEND)

        meta = json.dumps(meta, sort_keys=True).encode()
        os.write(self.fd, b'\n'.join([meta, data, b'']))


def backup(batch, batch_lock):
    logger = Logger(os.path.join(ARGS.volume_dir, 'logs'))

    while True:
        with batch_lock:
            active = batch['active']
            frozen = batch['frozen']

            batch['active'] = dict()
            batch['frozen'] = active

        # Work on the frozen batch now
        for k in frozen:
            blob_io(k*ARGS.block_size, frozen[k]['block'])

        time.sleep(1)

    while True:
        batch = None

        with locks['batch']:
            if shared_batch['batch']:
                # Take out the current batch
                # Writer would start using the next batch
                batch, shared_batch['batch'] = shared_batch['batch'], list()

        if batch:
            # Build a combined blob from all the pending writes
            body = list()
            for offset, octets, conn, response, ts in batch:
                body.append(struct.pack('!QQ', offset, len(octets)))
                body.append(octets)
            body = b''.join(body)

            # Upload the octets to log and log_index to details.json
            lsn += 1
            s3.create('logs/{}'.format(lsn), body)

            with locks['volume']:
                # Take the lock before updating the snapshot to ensure
                # that read requests do not send garbled data
                for offset, octets, conn, response, ts in batch:
                    snapshot_fd.seek(offset, os.SEEK_SET)
                    snapshot_fd.write(octets)

            os.fsync(snapshot_fd.fileno())

            with locks['send']:
                # Everything done, acknowledge the write request
                for offset, octets, conn, response, ts in batch:
                    conn.sendall(response)
                    log('write(%d) offset(%d) length(%d) msec(%d)',
                        lsn, offset, len(octets), (time.time()-ts)*1000)
        else:
            time.sleep(0.01)


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


def ASSERT(condition):
    assert condition

    if not condition:
        log('assert triggered. exiting...')
        os._exit(1)


blob_fd_cache = dict()


def blob_io(offset, block=None):
    blob_size = ARGS.blocks_per_blob * ARGS.block_size

    blob_num = (offset // blob_size) * blob_size

    blob_path = os.path.join(ARGS.volume_dir, 'objs', str(blob_num))

    if blob_path is None and block is None:
        return b'\0' * ARGS.block_size

    if blob_path not in blob_fd_cache:
        fd = os.open(blob_path, os.O_CREAT | os.O_RDWR)
        blob_fd_cache[blob_path] = fd

    fd = blob_fd_cache[blob_path]

    block_index = (offset - blob_num) // ARGS.block_size
    blob_offset = block_index * (ARGS.block_size + 32)
    os.lseek(fd, blob_offset, os.SEEK_SET)

    if block:
        checksum = hashlib.sha256(block).digest()
        return os.write(fd, block + checksum)
    else:
        block = os.read(fd, ARGS.block_size + 32)

        if hashlib.sha256(block[:-32]).digest() != block[-32:]:
            for b in block:
                if b != 0:
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
        ASSERT(0x25609513 == magic)           # Valid request header
        ASSERT(cmd in (0, 1))                 # Only 0:read or 1:write

        ASSERT(offset % ARGS.block_size == 0)
        ASSERT(length % ARGS.block_size == 0)

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
                    else:
                        block = blob_io(j*ARGS.block_size)

                if 0 == len(block):
                    block = b'\0' * ARGS.block_size

                blocks.append(block)

            conn.sendall(response_header)
            conn.sendall(b''.join(blocks))

            log('read offset(%d) length(%d) msec(%d)',
                offset, length, (time.time()-ts)*1000)

        # WRITE
        elif 1 == cmd:
            octets = recvall(conn, length)

            for i in range(block_count):
                block = octets[i*ARGS.block_size:(i+1)*ARGS.block_size]
                sha256=hashlib.sha256(block).hexdigest()

                with batch_lock:
                    batch['active'][block_offset+i] = dict(
                        block=block,
                        length=length,
                        sha256=sha256)

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
    batch = dict(batch=list())
    locks = dict(send=threading.Lock(),
                 batch=threading.Lock(),
                 volume=threading.Lock())

    s3 = S3(ARGS.prefix,
            os.environ['CBD_S3_ENDPOINT'], os.environ['CBD_S3_BUCKET'],
            os.environ['CBD_S3_AUTH_KEY'], os.environ['CBD_S3_AUTH_SECRET'])

    # Read the volume profile from S3
    octets = s3.read('profile.json')
    if octets is None:
        s3.create('profile.json', json.dumps(dict(
            uuid=str(uuid.uuid4()),
            block_size=ARGS.block_size,
            block_count=ARGS.block_count)).encode())

        octets = s3.read('profile.json')

    profile = json.loads(octets.decode())

    # Validate the block_size and block_count
    # This is required to avoid corrupting data
    if ((profile['block_size'] != ARGS.block_size) or
       (profile['block_count'] != ARGS.block_count)):

        log('block size or count mismatch')
        os._exit(1)

    if -1 == s3.snapshot_max:
        # Binary mode, truncate if it exists
        snapshot_fd = open('cbd.snapshot.' + profile['uuid'], 'wb+')
        snapshot_fd.seek(ARGS.block_size*ARGS.block_count)
        snapshot_fd.write(struct.pack('!Q', 0))
    else:
        snapshot_fd = open('cbd.snapshot.' + profile['uuid'], 'r+b')

    snapshot_fd.seek(ARGS.block_size*ARGS.block_count)
    synced = struct.unpack('!Q', snapshot_fd.read(8))[0]
    lsn = synced + 1

    for n in range(lsn, s3.log_max+1):
        body = s3.read('logs/{}'.format(n))

        i = 0
        while i < len(body):
            offset, length = struct.unpack('!QQ', body[i:i+16])
            octets = body[i+16:i+16+length]
            i += 16 + length

            snapshot_fd.seek(offset)
            snapshot_fd.write(octets)

            synced = n
            log('volume({}) lsn({}) offset({}) length({})'.format(
                ARGS.prefix, n, offset, length))

    os.fsync(snapshot_fd.fileno())
    snapshot_fd.seek(ARGS.block_size*ARGS.block_count)
    snapshot_fd.write(struct.pack('!Q', synced))
    """

    os.makedirs(os.path.join(ARGS.volume_dir, 'logs'), exist_ok=True)
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
