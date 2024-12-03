import os
import time
import fcntl
import struct
import socket
import hashlib
import threading
from logging import critical as log


class G:
    conn = None
    blobs = list()
    mutex = threading.Lock()
    snapshot = None
    responses = list()


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


def backup():
    while True:
        blobs = None

        with G.mutex:
            if G.blobs:
                blobs, G.blobs = G.blobs, list()
                responses, G.responses = G.responses, list()

        if blobs:
            log('size({})'.format(sum([len(b[1]) for b in blobs])))

            with G.mutex:
                for offset, octets in blobs:
                    os.lseek(G.snapshot, offset, os.SEEK_SET)
                    os.write(G.snapshot, octets)

            for response in responses:
                # G.blobs.append(struct.pack('!QQ', offset, length))
                G.conn.sendall(response)
        else:
            time.sleep(0.1)


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


def server(socket_path):
    if os.path.exists(socket_path):
        os.remove(socket_path)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(socket_path)
    sock.listen(1)

    conn, peer = sock.accept()
    G.conn = conn

    log('Connection received from {}'.format(peer))

    request_magic = 0x25609513
    response_magic = 0x67446698

    while True:
        magic, flags, cmd, cookie, offset, length = struct.unpack(
            '!IHHQQI', recvall(conn, 28))

        if magic != request_magic:
            raise Exception('Invalid Magic Number : 0x{:8x}'.format(magic))

        if cmd not in (0, 1):
            raise Exception('Invalid CMD : {}'.format(cmd))

        cmd = 'read' if 0 == cmd else 'write'

        log('%5s(%d) length(%d)', cmd, offset, length)

        # Read request
        if 'read' == cmd:
            conn.sendall(struct.pack('!IIQ', response_magic, 0, cookie))

            with G.mutex:
                os.lseek(G.snapshot, offset, os.SEEK_SET)
                octets = os.read(G.snapshot, length)

            conn.sendall(octets)

        # Write request
        if 'write' == cmd:
            octets = recvall(conn, length)

            with G.mutex:
                G.blobs.append((offset, octets))
                G.responses.append(struct.pack('!IIQ', response_magic, 0,
                                               cookie))


def main(device_path, block_size, block_count, timeout, snapshot_path):
    G.snapshot = os.open(snapshot_path, os.O_RDWR)
    fcntl.flock(G.snapshot, fcntl.LOCK_EX)

    socket_path = hashlib.md5(device_path.encode() + snapshot_path.encode())
    socket_path = os.path.join('/tmp', 'cbd.' + socket_path.hexdigest())

    threading.Thread(target=server, args=(socket_path,)).start()
    threading.Thread(target=backup).start()

    device_init(device_path, block_size, block_count, timeout, socket_path)
