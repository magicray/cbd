import os
import time
import boto3
import fcntl
import struct
import socket
import hashlib
import threading
from logging import critical as log


class G:
    s3 = None
    conn = None
    batch = list()
    bucket = None
    folder = None
    snapshot = None
    log_index = None
    device_size = None
    send_lock = threading.Lock()
    batch_lock = threading.Lock()
    snapshot_lock = threading.Lock()


def backup():
    while True:
        batch = None

        with G.batch_lock:
            if G.batch:
                # Take out the current batch
                # Writer would start using the next batch
                batch, G.batch = G.batch, list()

        if batch:
            G.log_index += 1

            # Build a combined blob from all the pending writes
            body = list()
            for offset, octets, response in batch:
                body.append(struct.pack('!QQ', offset, len(octets)))
                body.append(octets)
            body = b''.join(body)

            # Upload it to Object Store
            G.s3.put_object(
                Bucket=G.bucket,
                Key=G.folder + '/logs/' + str(G.log_index),
                Body=body,
                ContentType='application/octet-stream')

            log('uploaded({}) size({})'.format(G.log_index, len(body)))

            with G.snapshot_lock:
                # Take the lock before updating the snapshot to ensure
                # that read request does not send garbled data
                for offset, octets, response in batch:
                    os.lseek(G.snapshot, offset, os.SEEK_SET)
                    os.write(G.snapshot, octets)

                os.lseek(G.snapshot, G.device_size, os.SEEK_SET)
                os.write(G.snapshot, struct.pack('!Q', G.log_index))
                os.fsync(G.snapshot)

            with G.send_lock:
                # Everything done
                # We can acknowledge the write request now
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

        # READ - send the data from the snapshot
        if 0 == cmd:
            with G.snapshot_lock:
                os.lseek(G.snapshot, offset, os.SEEK_SET)
                octets = os.read(G.snapshot, length)
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


def main(device_path, block_size, block_count, timeout, snapshot_path, s3path, max_log_index):
    G.snapshot = os.open(snapshot_path, os.O_RDWR)
    fcntl.flock(G.snapshot, fcntl.LOCK_EX)

    G.device_size = block_size * block_count
    assert (os.path.getsize(snapshot_path) == G.device_size + block_size)

    os.lseek(G.snapshot, G.device_size, os.SEEK_SET)
    G.log_index = struct.unpack('!Q', os.read(G.snapshot, 8))[0]
    log('snapshot({}) size({}) log_index({})'.format(
        snapshot_path, G.device_size, G.log_index))

    if s3path:
        tmp = s3path.split('/')
        endpoint, G.bucket, G.folder = '/'.join(tmp[:-2]), tmp[-2], tmp[-1]

        G.s3 = boto3.client(
            's3', endpoint_url=endpoint,
            aws_access_key_id='1DPFNzs3yeEyrQepgERD',
            aws_secret_access_key='GydnuHxjwtHHoNEEDyav7C2LRK2LbyHaeX9msnvg')

        for log_index in range(G.log_index+1, max_log_index+1):
            obj = G.s3.get_object(
                Bucket=G.bucket,
                Key=G.folder + '/logs/' + str(log_index))
            body = obj['Body'].read()

            assert (len(body) == obj['ContentLength'])

            i = 0
            while i < len(body):
                offset, length = struct.unpack('!QQ', body[i:i+16])
                octets = body[i+16:i+16+length]
                i += 16 + length

                os.lseek(G.snapshot, offset, os.SEEK_SET)
                os.write(G.snapshot, octets)

                log('log_index({}) offset({}) length({})'.format(
                    log_index, offset, length))

        os.lseek(G.snapshot, G.device_size, os.SEEK_SET)
        os.write(G.snapshot, struct.pack('!Q', max_log_index))
        os.fsync(G.snapshot)

        G.log_index = max_log_index + 1

    socket_path = hashlib.md5(device_path.encode() + snapshot_path.encode())
    socket_path = os.path.join('/tmp', 'cbd.' + socket_path.hexdigest())

    threading.Thread(target=server, args=(socket_path, G.device_size)).start()
    threading.Thread(target=backup).start()

    device_init(device_path, block_size, block_count, timeout, socket_path)
