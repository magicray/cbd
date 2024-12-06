import os
import json
import time
import boto3
import fcntl
import struct
import socket
import hashlib
import logging
import argparse
import threading
from logging import critical as log


class G:
    s3 = None
    vol = None
    conn = None
    batch = list()
    volume = None
    log_index = None
    device_size = None
    send_lock = threading.Lock()
    batch_lock = threading.Lock()
    volume_lock = threading.Lock()
    batch_event = threading.Event()


class S3Bucket:
    def __init__(self, s3bucket, key_id, secret_key):
        tmp = s3bucket.split('/')
        self.bucket = tmp[-1]
        self.endpoint = '/'.join(tmp[:-1])

        self.s3 = boto3.client('s3', endpoint_url=self.endpoint,
                               aws_access_key_id=key_id,
                               aws_secret_access_key=secret_key)

    def get(self, key):
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        octets = obj['Body'].read()
        assert (len(octets) == obj['ContentLength'])
        return octets

    def put(self, key, value, content_type='application/octet-stream'):
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=value,
                           ContentType=content_type)


def backup():
    while True:
        batch = None

        with G.batch_lock:
            if G.batch:
                # Take out the current batch
                # Writer would start using the next batch
                batch, G.batch = G.batch, list()
                G.batch_event.clear()

        if batch:
            G.log_index += 1

            # Build a combined blob from all the pending writes
            body = list()
            for offset, octets, response in batch:
                body.append(struct.pack('!QQ', offset, len(octets)))
                body.append(octets)
            body = b''.join(body)

            if G.s3:
                # Upload the octets to log and log_index to details.json
                G.s3.put(G.volume + '/logs/' + str(G.log_index), body)
                G.s3.put(G.volume + '/details.json',
                         json.dumps(dict(log_index=G.log_index)),
                         'application/json')

                log('uploaded({}) size({})'.format(G.log_index, len(body)))

            with G.volume_lock:
                # Take the lock before updating the snapshot to ensure
                # that read request does not send garbled data
                for offset, octets, response in batch:
                    os.lseek(G.vol, offset, os.SEEK_SET)
                    os.write(G.vol, octets)

                os.fsync(G.vol)

                if G.s3:
                    os.lseek(G.vol, G.device_size, os.SEEK_SET)
                    os.write(G.vol, struct.pack('!Q', G.log_index))
                    os.fsync(G.vol)

            with G.send_lock:
                # Everything done
                # We can acknowledge the write request now
                for offset, octets, response in batch:
                    G.conn.sendall(response)
        else:
            log('waiting for next write batch')
            G.batch_event.wait()


def recvall(socket, length):
    buf = list()
    while length:
        octets = socket.recv(length)

        if not octets:
            socket.close()
            raise Exception('Connection closed')

        buf.append(octets)
        length -= len(octets)

    return b''.join(buf)


def server(socket_path, device_size):
    if os.path.exists(socket_path):
        os.remove(socket_path)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(socket_path)
    sock.listen(1)

    G.conn, peer = sock.accept()
    log('Connection received')

    while True:
        magic, flags, cmd, cookie, offset, length = struct.unpack(
            '!IHHQQI', recvall(G.conn, 28))

        assert (0x25609513 == magic)           # Valid request header
        assert (cmd in (0, 1))                 # Only 0:read or 1:write
        assert (offset+length <= device_size)  # Device size limit

        log('cmd(%d) offset(%d) length(%d)', cmd, offset, length)

        # Response header is common. No errors are supported.
        response_header = struct.pack('!IIQ', 0x67446698, 0, cookie)

        # READ - send the data from the volume
        if 0 == cmd:
            with G.volume_lock:
                os.lseek(G.vol, offset, os.SEEK_SET)
                octets = os.read(G.vol, length)
                assert (len(octets) == length)

            with G.send_lock:
                G.conn.sendall(response_header)
                G.conn.sendall(octets)

        # WRITE - put the required data in the next batch
        # Backup thread would store the entire batch on the
        # cloud and then send the response back.
        if 1 == cmd:
            octets = recvall(G.conn, length)

            with G.batch_lock:
                G.batch.append((offset, octets, response_header))
                G.batch_event.set()


class NBD:
    # (0xab << 8) + 0
    SET_SOCK = 43776
    SET_BLKSIZE = 43777
    SET_SIZE = 43778
    DO_IT = 43779
    CLEAR_SOCK = 43780
    CLEAR_QUEUE = 43781
    PRINT_DEBUG = 43782
    SET_SIZE_BLOCKS = 43783
    DISCONNECT = 43784
    SET_TIMEOUT = 43785
    SET_FLAGS = 43786


def device_init(dev, block_size, block_count, timeout, socket_path):
    fd = os.open(dev, os.O_RDWR)
    fcntl.ioctl(fd, NBD.CLEAR_QUEUE)
    fcntl.ioctl(fd, NBD.DISCONNECT)
    fcntl.ioctl(fd, NBD.CLEAR_SOCK)
    fcntl.ioctl(fd, NBD.SET_BLKSIZE, block_size)
    fcntl.ioctl(fd, NBD.SET_SIZE_BLOCKS, block_count)
    fcntl.ioctl(fd, NBD.SET_TIMEOUT, timeout)
    fcntl.ioctl(fd, NBD.PRINT_DEBUG)
    log('initialized(%s) block_size(%d) block_count(%d)',
        dev, block_size, block_count)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    while True:
        try:
            sock.connect(socket_path)
            log('connected(%s) socket(%s)', dev, socket_path)
            break
        except Exception as e:
            log(e)
            time.sleep(1)

    fcntl.ioctl(fd, NBD.SET_SOCK, sock.fileno())
    fcntl.ioctl(fd, NBD.DO_IT)


def main(device_path, block_size, block_count, timeout, volume_path, s3bucket):
    if volume_path:
        if not os.path.isfile(volume_path):
            fd = os.open(volume_path, os.O_CREAT | os.O_RDWR)
            os.write(fd, b'\x00' * block_size)
            os.lseek(fd, block_size * block_count, os.SEEK_SET)
            os.write(fd, struct.pack('!Q', 0))
            os.fsync(fd)
            os.close(fd)

        G.vol = os.open(volume_path, os.O_RDWR)
        fcntl.flock(G.vol, fcntl.LOCK_EX)
        G.volume = os.path.basename(volume_path)

        G.device_size = block_size * block_count
        assert (os.path.getsize(volume_path) >= G.device_size + 8)

        os.lseek(G.vol, G.device_size, os.SEEK_SET)
        G.log_index = struct.unpack('!Q', os.read(G.vol, 8))[0]
        log('volume({}) size({}) log_index({})'.format(
            volume_path, G.device_size, G.log_index))

    if s3bucket and volume_path:
        G.s3 = S3Bucket(s3bucket, '1DPFNzs3yeEyrQepgERD',
                        'GydnuHxjwtHHoNEEDyav7C2LRK2LbyHaeX9msnvg')

        details = json.loads(G.s3.get(G.volume + '/details.json'))

        for log_index in range(G.log_index+1, details['log_index']+1):
            body = G.s3.get(G.volume + '/logs/' + str(log_index))

            i = 0
            while i < len(body):
                offset, length = struct.unpack('!QQ', body[i:i+16])
                octets = body[i+16:i+16+length]
                i += 16 + length

                os.lseek(G.vol, offset, os.SEEK_SET)
                os.write(G.vol, octets)

                log('log_index({}) offset({}) length({})'.format(
                    log_index, offset, length))

        os.fsync(G.vol)
        os.lseek(G.vol, G.device_size, os.SEEK_SET)
        os.write(G.vol, struct.pack('!Q', details['log_index']))
        os.fsync(G.vol)

        G.log_index = details['log_index']

    if volume_path:
        socket_path = hashlib.md5(device_path.encode() + volume_path.encode())
        socket_path = os.path.join('/tmp', 'cbd.' + socket_path.hexdigest())

        args = (socket_path, G.device_size)
        threading.Thread(target=server, args=args).start()
        threading.Thread(target=backup).start()

    if device_path and block_size and block_count and socket_path:
        device_init(device_path, block_size, block_count, timeout, socket_path)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()

    ARGS.add_argument(
        '--device',
        help='Network Block Device path')

    ARGS.add_argument(
        '--block_size', type=int, default=4096,
        help='Device Block Size')

    ARGS.add_argument(
        '--block_count', type=int, default=256*1024,
        help='Device Block Count')

    ARGS.add_argument(
        '--timeout', type=int, default=60,
        help='Timeout in seconds')

    ARGS.add_argument(
        '--volume', default='volume',
        help='File for keeping a volume')

    ARGS.add_argument(
        '--s3bucket',
        help='s3bucket for this volume')

    ARGS = ARGS.parse_args()

    main(ARGS.device, ARGS.block_size, ARGS.block_count,
         ARGS.timeout, ARGS.volume, ARGS.s3bucket)
