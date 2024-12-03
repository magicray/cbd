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
    batch = list()
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


def backup():
    while True:
        batch = None

        with G.mutex:
            if G.batch:
                # Take out the current batch
                # Writer would start using the next batch
                batch, G.batch = G.batch, list()

        if batch:
            # Upload it to Object Store
            # for offset, octets, _ in batch:
            # struct.pack('!QQ', offset, len(octets))
            log('size({})'.format(sum([len(b[1]) for b in batch])))

            with G.mutex:
                # Take the lock before updating the snapshot to ensure
                # that read request does not send garbled data
                for offset, octets, response in batch:
                    os.lseek(G.snapshot, offset, os.SEEK_SET)
                    os.write(G.snapshot, octets)

            # Everything done. We can acknowledge the write request now
            for offset, octets, response in batch:
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

    while True:
        magic, flags, cmd, cookie, offset, length = struct.unpack(
            '!IHHQQI', recvall(conn, 28))

        # Validate that it is a valid NBD request
        if 0x25609513 != magic:
            raise Exception('Invalid Magic Number : 0x{:8x}'.format(magic))

        # We support only 0:read and 1:write
        if cmd not in (0, 1):
            raise Exception('Invalid CMD : {}'.format(cmd))

        log('cmd(%d) offset(%d) length(%d)', cmd, offset, length)

        # Response header is common. No errors are supported.
        response_header = struct.pack('!IIQ', 0x67446698, 0, cookie)

        # Handle the read request
        # Send the data from the snapshot
        if 0 == cmd:
            conn.sendall(response_header)

            with G.mutex:
                os.lseek(G.snapshot, offset, os.SEEK_SET)
                octets = os.read(G.snapshot, length)

            conn.sendall(octets)

        # Handle the write request
        # Put the required data the next batch
        # Backup thread would store the entire batch on the
        # cloud and then only send the response back.
        if 1 == cmd:
            octets = recvall(conn, length)

            with G.mutex:
                G.batch.append((offset, octets, response_header))


def main(device_path, block_size, block_count, timeout, snapshot_path):
    G.snapshot = os.open(snapshot_path, os.O_RDWR)
    fcntl.flock(G.snapshot, fcntl.LOCK_EX)

    socket_path = hashlib.md5(device_path.encode() + snapshot_path.encode())
    socket_path = os.path.join('/tmp', 'cbd.' + socket_path.hexdigest())

    threading.Thread(target=server, args=(socket_path,)).start()
    threading.Thread(target=backup).start()

    device_init(device_path, block_size, block_count, timeout, socket_path)
