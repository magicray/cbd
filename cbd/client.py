import os
import json
import time
import fcntl
import struct
import socket
import logging
import argparse
from logging import critical as log


# Network Block Device ioctl commands
# Value = (0xab << 8) + n
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


def main(dev, block_size, block_count, timeout, volume_id, host, port):
    fd = os.open(dev, os.O_RDWR)
    fcntl.ioctl(fd, NBD_CLEAR_QUEUE)
    fcntl.ioctl(fd, NBD_DISCONNECT)
    fcntl.ioctl(fd, NBD_CLEAR_SOCK)
    fcntl.ioctl(fd, NBD_SET_BLKSIZE, block_size)
    fcntl.ioctl(fd, NBD_SET_SIZE_BLOCKS, block_count)
    fcntl.ioctl(fd, NBD_SET_TIMEOUT, timeout)
    fcntl.ioctl(fd, NBD_PRINT_DEBUG)
    log('initialized(%s) block_size(%d) block_count(%d)',
        dev, block_size, block_count)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    while True:
        try:
            sock.connect((host, port))
            details = dict(volume_id=volume_id, size=block_size*block_count)
            octets = json.dumps(details).encode()
            sock.sendall(struct.pack('!Q', len(octets)))
            sock.sendall(octets)
            log('connected(%s) volume_id(%d) size(%d) host(%s) port(%s)',
                dev, volume_id, details['size'], host, port)
            break
        except Exception as e:
            log(e)
            time.sleep(1)

    fcntl.ioctl(fd, NBD_SET_SOCK, sock.fileno())
    fcntl.ioctl(fd, NBD_DO_IT)


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
        '--volume_id', type=int,
        help='Volume ID - integer that identifies the volume')

    ARGS.add_argument(
        '--host', default='127.0.0.1',
        help='Hostname of the server')

    ARGS.add_argument(
        '--port', type=int, default='9876',
        help='Port number of the server')

    ARGS = ARGS.parse_args()

    main(ARGS.device, ARGS.block_size, ARGS.block_count,
         ARGS.timeout, ARGS.volume_id, ARGS.host, ARGS.port)
