import os
import sys
import time
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


def device_init(dev, block_size, block_count, timeout, ip, port):
    fd = os.open(dev, os.O_RDWR)
    fcntl.ioctl(fd, NBD.CLEAR_QUEUE)
    fcntl.ioctl(fd, NBD.DISCONNECT)
    fcntl.ioctl(fd, NBD.CLEAR_SOCK)
    log('opened(%s)', dev)

    fcntl.ioctl(fd, NBD.SET_BLKSIZE, block_size)
    fcntl.ioctl(fd, NBD.SET_SIZE_BLOCKS, block_count)
    fcntl.ioctl(fd, NBD.SET_TIMEOUT, timeout)
    log('initialized(%s) block_size(%d) block_count(%d)',
        dev, block_size, block_count)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip, port))
    log('connected(%s) ip(%s) port(%d)', dev, ip, port)

    fcntl.ioctl(fd, NBD.SET_SOCK, sock.fileno())
    fcntl.ioctl(fd, NBD.DO_IT)


async def server(reader, writer):
    peer = writer.get_extra_info('socket').getpeername()
    log('connection from %s', peer)

    blocks = dict()
    request_magic = 0x25609513
    response_magic = 0x67446698

    while True:
        try:
            magic, flags, cmd, cookie, offset, length = struct.unpack(
                '!IHHQQI', await reader.readexactly(28))

            if magic != request_magic:
                raise Exception('Invalid Magic Number : 0x{:8x}'.format(magic))

            if cmd not in (0, 1):
                raise Exception('Invalid CMD : {}'.format(cmd))

            if offset % ARGS.block_size:
                raise Exception('Invalid Offset : {:d}'.format(offset))

            if length % ARGS.block_size:
                raise Exception('Invalid Length : {:d}'.format(length))

            cmd = 'read' if 0 == cmd else 'write'
            block_index = offset // ARGS.block_size
            block_count = length // ARGS.block_size

            log('%s block_index(%d) block_count(%d)', cmd, block_index, block_count)

            # Read request
            if 'read' == cmd:
                writer.write(struct.pack('!IIQ', response_magic, 0, cookie))
                
                for i in range(block_count):
                    writer.write(blocks.get(block_index + i, b'A' * ARGS.block_size))

            # Write request
            if 'write'  == cmd:
                for i in range(block_count):
                    blocks[block_index + i] = await reader.readexactly(ARGS.block_size)

                writer.write(struct.pack('!IIQ', response_magic, 0, cookie))
        except Exception as e:
            writer.close()
            log('closed{} {}'.format(peer, e))
            log('magic(0x%08x) flags(0x%04x) cmd(0x%04x) cookie(0x%016x) offset(%d) length(%d)',
                magic, flags, cmd, cookie, offset, length)
            return


def server_thread(ip, port):
    async def start_server():
        srv = await asyncio.start_server(server, ip, port)
        async with srv:
            await srv.serve_forever()

    asyncio.run(start_server())
    log('server exited')
    sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()

    ARGS.add_argument('--device', default='/dev/nbd0',
        help='Network Block Device path')
    ARGS.add_argument('--block_size', type=int, default=4096,
        help='Device Block Size')
    ARGS.add_argument('--block_count', type=int, default=256*1024,
        help='Device Block Count')
    ARGS.add_argument('--timeout', type=int, default=60,
        help='Timeout in seconds')
    ARGS.add_argument('--ip', default='localhost',
        help='Server IP Address')
    ARGS.add_argument('--port', type=int, default=5000,
        help='Server Port')

    ARGS = ARGS.parse_args()

    threading.Thread(target=server_thread, args=(ARGS.ip, ARGS.port)).start()
    time.sleep(2)
    device_init(ARGS.device, ARGS.block_size, ARGS.block_count, ARGS.timeout,
                ARGS.ip, ARGS.port)
