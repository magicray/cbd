import os
import time
import json
import uuid
import boto3
import fcntl
import struct
import socket
import logging
import argparse
import threading
from logging import critical as log


def device_init(dev, block_size, block_count, conn):
    # Value = (0xab << 8) + n
    # Network Block Device ioctl commands
    NBD_SET_SOCK = 43776
    NBD_SET_BLKSIZE = 43777
    NBD_SET_SIZE = 43778
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


def backup(s3, snapshot_fd, lsn, shared_batch, locks):
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
    blob_num = (offset // ARGS.blob_size) * ARGS.blob_size

    if block:
        blob_path = os.path.join(ARGS.volume_dir, 'latest', str(blob_num))
    else:
        dirs = ['latest']
        checkpoints = os.listdir(ARGS.volume_dir)
        dirs.extend(sorted([int(d) for d in checkpoints if d.isdigit()]))
        dirs.extend(['oldest'])

        blob_path = None
        for d in dirs:
            tmp = os.path.join(ARGS.volume_dir, d, str(blob_num))
            if os.path.isfile(tmp):
                blob_path = tmp
                break

    if blob_path is None and block is None:
        return b'\0' * ARGS.block_size

    if blob_path not in blob_fd_cache:
        if block:
            fd = os.open(blob_path, os.O_CREAT | os.O_RDWR)
        else:
            fd = os.open(blob_path, os.O_RDWR)

        blob_fd_cache[blob_path] = fd

    fd = blob_fd_cache[blob_path]
    os.lseek(fd, offset-blob_num, os.SEEK_SET)

    if block:
        return os.write(fd, block)
    else:
        return os.read(fd, ARGS.block_size)


def server(sock, s3, snapshot_fd, batch, locks):
    conn, peer = sock.accept()
    log('client connection accepted')

    os.makedirs(os.path.join(ARGS.volume_dir, 'latest'), exist_ok=True)
    os.makedirs(os.path.join(ARGS.volume_dir, 'oldest'), exist_ok=True)

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

        # READ
        if 0 == cmd:
            blocks = list()

            for i in range(length // ARGS.block_size):
                blocks.append(blob_io(offset + i*ARGS.block_size))

                if 0 == len(blocks[-1]):
                    blocks[-1] = b'\0' * ARGS.block_size

                ASSERT(len(blocks[-1]) == ARGS.block_size)

            conn.sendall(response_header)
            conn.sendall(b''.join(blocks))

            log('read offset(%d) length(%d) msec(%d)',
                offset, length, (time.time()-ts)*1000)

        # WRITE
        elif 1 == cmd:
            octets = recvall(conn, length)

            for i in range(len(octets) // ARGS.block_size):
                block = octets[i*ARGS.block_size : (i+1)*ARGS.block_size]

                blob_io(offset+i*ARGS.block_size, block)

            conn.sendall(response_header)

            log('write offset(%d) length(%d) msec(%d)',
                offset, length, (time.time()-ts)*1000)
        else:
            log('cmd(%d) offset(%d) length(%d) msec(%d)',
                cmd, offset, length, (time.time()-ts)*1000)
            os._exit(0)


def main():
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

    # Start the backup thread
    #args = (s3, snapshot_fd, synced, batch, locks)
    #threading.Thread(target=backup, args=args).start()

    # Initialize the unix domain server socket
    sock_path = os.path.join('/tmp', str(uuid.uuid4()))
    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_sock.bind(sock_path)
    server_sock.listen(1)
    log('server listening on sock(%s)', sock_path)

    # Start the server thread
    s3 = None
    snapshot_fd = None
    batch = None
    locks = None
    args = (server_sock, s3, snapshot_fd, batch, locks)
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

    ARGS.add_argument('--blob_size', default=64*1024*1024,
        help='blob size')

    ARGS = ARGS.parse_args()

    ASSERT(ARGS.blob_size % ARGS.block_size == 0)

    main()
