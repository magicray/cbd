import os
import sys
import time
import mmap
import uuid
import fcntl
import struct
import socket
import asyncio
import logging
import argparse
import threading
from logging import critical as log


class NBD:
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


async def replicator():
    while True:
        blobs, ARGS.blobs = ARGS.blobs, list()

        size = 0
        for b in blobs:
            size += len(b)
        log('size {}'.format(size))

        await asyncio.sleep(1)


async def server(reader, writer):
    peer = writer.get_extra_info('socket').getpeername()
    log('connection from %s', peer)

    request_magic = 0x25609513
    response_magic = 0x67446698

    while True:
        magic, flags, cmd, cookie, offset, length = struct.unpack(
            '!IHHQQI', await reader.readexactly(28))

        if magic != request_magic:
            raise Exception('Invalid Magic Number : 0x{:8x}'.format(magic))

        if cmd not in (0, 1):
            raise Exception('Invalid CMD : {}'.format(cmd))

        cmd = 'read' if 0 == cmd else 'write'

        log('%5s offset(%d) length(%d)', cmd, offset, length)

        # Read request
        if 'read' == cmd:
            writer.write(struct.pack('!IIQ', response_magic, 0, cookie))

            os.lseek(ARGS.fd, offset, os.SEEK_SET)

            writer.write(os.read(ARGS.fd, length))

        # Write request
        if 'write'  == cmd:
            octets = await reader.readexactly(length)

            ARGS.blobs.append(struct.pack('!QQ', offset, length))
            ARGS.blobs.append(octets)

            os.lseek(ARGS.fd, offset, os.SEEK_SET)
            os.write(ARGS.fd, octets)

            writer.write(struct.pack('!IIQ', response_magic, 0, cookie))


def server_thread(socket_path):
    async def start_server():
        srv = await asyncio.start_unix_server(server, socket_path)
        async with srv:
            await srv.serve_forever()

    loop = asyncio.new_event_loop()
    loop.create_task(start_server())
    loop.create_task(replicator())
    loop.run_forever()


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()

    ARGS.add_argument('--device',
        help='Network Block Device path')
    ARGS.add_argument('--cache_file', default='cache_file',
        help='Local file for caching')
    ARGS.add_argument('--block_size', type=int, default=4096,
        help='Device Block Size')
    ARGS.add_argument('--block_count', type=int, default=256*1024,
        help='Device Block Count')
    ARGS.add_argument('--timeout', type=int, default=60,
        help='Timeout in seconds')

    ARGS = ARGS.parse_args()
    ARGS.blobs = list()

    ARGS.fd = os.open(ARGS.cache_file, os.O_CREAT | os.O_RDWR)
    fcntl.flock(ARGS.fd, fcntl.LOCK_EX)

    ARGS.socket_path = os.path.join('/tmp', 'cbd-sock.' + uuid.uuid4().hex)
    threading.Thread(target=server_thread, args=(ARGS.socket_path,)).start()

    device_init(ARGS.device, ARGS.block_size, ARGS.block_count, ARGS.timeout,
                ARGS.socket_path)
