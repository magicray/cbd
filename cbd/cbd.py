import os
import sys
import time
import mmap
import uuid
import fcntl
import struct
import socket
import asyncio
import threading
from logging import critical as log


class G:
    blobs = list()
    mutex = threading.Lock()
    snapshot = None


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


def logger_thread():
    while True:
        blobs = None

        with G.mutex:
            if G.blobs:
                blobs, G.blobs = G.blobs, list()

        if blobs:
            log('size({})'.format(sum([len(b) for b in blobs])))
        else:
            time.sleep(1)


async def server(reader, writer):
    peer = writer.get_extra_info('socket').getpeername()
    log('connection received')

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

        log('%5s(%d) length(%d)', cmd, offset, length)

        # Read request
        if 'read' == cmd:
            writer.write(struct.pack('!IIQ', response_magic, 0, cookie))

            os.lseek(G.snapshot, offset, os.SEEK_SET)

            writer.write(os.read(G.snapshot, length))

        # Write request
        if 'write'  == cmd:
            octets = await reader.readexactly(length)

            with G.mutex:
                G.blobs.append(struct.pack('!QQ', offset, length))
                G.blobs.append(octets)
        
            os.lseek(G.snapshot, offset, os.SEEK_SET)
            os.write(G.snapshot, octets)

            writer.write(struct.pack('!IIQ', response_magic, 0, cookie))


def server_thread(socket_path, snapshot):
    G.snapshot = os.open(snapshot, os.O_RDWR)

    async def start_server():
        srv = await asyncio.start_unix_server(server, socket_path)
        async with srv:
            await srv.serve_forever()

    asyncio.run(start_server())
