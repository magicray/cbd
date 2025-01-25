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

        log('endpoint(%s) bucket(%s) log(%s) log(%d, %d) snapshot(%d, %d)',
            endpoint, bucket, prefix,
            self.log_min, self.log_max,
            self.snapshot_min, self.snapshot_max)

    def create(self, key, value):
        ts = time.time()
        key = os.path.join(self.prefix, key)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=value,
                           IfNoneMatch='*')
        log('s3(%s) bucket(%s) create(%s) length(%d) msec(%d)',
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

        log('s3(%s) bucket(%s) read(%s) length(%d) msec(%d)',
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
            for offset, octets, conn, response in batch:
                body.append(struct.pack('!QQ', offset, len(octets)))
                body.append(octets)
            body = b''.join(body)

            # Upload the octets to log and log_index to details.json
            lsn += 1
            s3.create('logs/{}'.format(lsn), body)

            with locks['volume']:
                # Take the lock before updating the snapshot to ensure
                # that read requests do not send garbled data
                for offset, octets, conn, response in batch:
                    snapshot_fd.seek(offset, os.SEEK_SET)
                    snapshot_fd.write(octets)

            with locks['send']:
                # Everything done
                # We can acknowledge the write request now
                for offset, octets, conn, response in batch:
                    conn.sendall(response)
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


def server(sock, s3, snapshot_fd, batch, locks):
    conn, peer = sock.accept()
    log('client connection accepted')

    while True:
        magic, flags, cmd, cookie, offset, length = struct.unpack(
            '!IHHQQI', recvall(conn, 28))

        assert (0x25609513 == magic)           # Valid request header
        assert (cmd in (0, 1))                 # Only 0:read or 1:write

        log('cmd(%d) offset(%d) length(%d)', cmd, offset, length)

        # Response header is common. No errors are supported.
        response_header = struct.pack('!IIQ', 0x67446698, 0, cookie)

        # READ - send the data from the volume
        if 0 == cmd:
            with locks['volume']:
                snapshot_fd.seek(offset, os.SEEK_SET)
                octets = snapshot_fd.read(length)
                assert (len(octets) == length)

            with locks['send']:
                conn.sendall(response_header)
                conn.sendall(octets)

        # WRITE - put the required data in the next batch
        # Backup thread would store the entire batch on the
        # cloud and then send the response back.
        if 1 == cmd:
            octets = recvall(conn, length)

            with locks['batch']:
                batch['batch'].append((offset, octets, conn, response_header))


def main():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()

    ARGS.add_argument(
        '--device', default='/dev/nbd0',
        help='Network Block Device path')

    ARGS.add_argument(
        '--block_size', type=int, default=4096,
        help='Device Block Size')

    ARGS.add_argument(
        '--block_count', type=int, default=256*1024,
        help='Device Block Count')

    ARGS.add_argument(
        '--prefix', default='test/default',
        help='path prefix in s3 bucket')

    ARGS = ARGS.parse_args()

    batch = dict(batch=list())
    locks = dict(send=threading.Lock(),
                 batch=threading.Lock(),
                 volume=threading.Lock())

    # Validate the requested parameter
    # This is required to avoid overwriting data
    s3 = S3(ARGS.prefix,
            os.environ['CBD_S3_ENDPOINT'], os.environ['CBD_S3_BUCKET'],
            os.environ['CBD_S3_AUTH_KEY'], os.environ['CBD_S3_AUTH_SECRET'])

    for i in range(2):
        octets = s3.read('profile.json')
        if octets is None:
            octets = json.dumps(dict(
                uuid=str(uuid.uuid4()),
                block_size=ARGS.block_size,
                block_count=ARGS.block_count)).encode()

            s3.create('profile.json', octets)
        else:
            profile = json.loads(octets.decode())
            break

    if ((profile['block_size'] != ARGS.block_size) or
       (profile['block_count'] != ARGS.block_count)):

        log('block size or count mismatch')
        os._exit(1)

    if -1 == s3.snapshot_max:
        # Binary mode, truncate if it exists
        snapshot_fd = open('cbd.snapshot.' + profile['uuid'], 'wb+')
        snapshot_fd.seek(ARGS.block_size*ARGS.block_count-1)
        snapshot_fd.write(b'0')

    lsn = 0
    for lsn in range(s3.log_min, s3.log_max+1):
        body = s3.read('logs/{}'.format(lsn))

        i = 0
        while i < len(body):
            offset, length = struct.unpack('!QQ', body[i:i+16])
            octets = body[i+16:i+16+length]
            i += 16 + length

            snapshot_fd.seek(offset)
            snapshot_fd.write(octets)

            log('volume({}) lsn({}) offset({}) length({})'.format(
                ARGS.prefix, lsn, offset, length))

    os.fsync(snapshot_fd.fileno())

    # Start the server thread
    sock_path = os.path.join('/tmp', str(uuid.uuid4()))
    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_sock.bind(sock_path)
    server_sock.listen(1)
    log('server listening on sock(%s)', sock_path)

    args = (server_sock, s3, snapshot_fd, batch, locks)
    threading.Thread(target=server, args=args).start()

    # Start the backup thread
    args = (s3, snapshot_fd, lsn, batch, locks)
    threading.Thread(target=backup, args=args).start()

    client_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client_sock.connect(sock_path)
    os.remove(sock_path)

    # Initialize the device and block
    device_init(ARGS.device, ARGS.block_size, ARGS.block_count,
                client_sock.fileno())


if __name__ == '__main__':
    main()
