import os
import json
import time
import boto3
import struct
import socket
import logging
import argparse
import threading
from logging import critical as log


class G:
    s3 = None
    logs = None
    conn = None
    batch = list()
    volumes = dict()
    volumes_dir = None
    send_lock = threading.Lock()
    batch_lock = threading.Lock()
    volume_lock = threading.Lock()


class S3Bucket:
    def __init__(self, s3bucket, key_id, secret_key):
        tmp = s3bucket.split('/')
        self.bucket = tmp[-1]
        self.endpoint = '/'.join(tmp[:-1])

        self.s3 = boto3.client('s3', endpoint_url=self.endpoint,
                               aws_access_key_id=key_id,
                               aws_secret_access_key=secret_key)

    def get(self, key):
        key = 'Cloud Block Devices/' + key
        obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        octets = obj['Body'].read()
        assert (len(octets) == obj['ContentLength'])
        return octets

    def put(self, key, value, content_type='application/octet-stream'):
        key = 'Cloud Block Devices/' + key
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

        if batch:
            # Build a combined blob from all the pending writes
            body = list()
            for volume_id, offset, octets, conn, response in batch:
                body.append(struct.pack('!QQQ', volume_id,
                                        offset, len(octets)))
                body.append(octets)
            body = b''.join(body)

            if G.s3:
                # Upload the octets to log and log_index to details.json
                G.log['total'] += 1
                G.s3.put('logs/' + str(G.log['total']), body)
                G.s3.put('log.json', json.dumps(G.log), 'application/json')

                log('uploaded({}) size({})'.format(G.log['total'], len(body)))

            with G.volume_lock:
                # Take the lock before updating the snapshot to ensure
                # that read request does not send garbled data
                for vol_id, offset, octets, conn, response in batch:
                    os.lseek(G.volumes[vol_id], offset, os.SEEK_SET)
                    os.write(G.volumes[vol_id], octets)

            for volume_id, fd in G.volumes.items():
                os.fsync(fd)

            G.log['applied'] += 1

            with G.send_lock:
                # Everything done
                # We can acknowledge the write request now
                for vol_id, offset, octets, conn, response in batch:
                    try:
                        conn.sendall(response)
                    except Exception as e:
                        log('volume(%d) exception(%s)', vol_id, e)
        else:
            time.sleep(0.01)


def recvall(conn, length):
    buf = list()
    while length:
        octets = conn.recv(length)

        if not octets:
            conn.close()
            raise Exception('Connection closed')

        buf.append(octets)
        length -= len(octets)

    return b''.join(buf)


def server(conn):
    req = json.loads(recvall(conn, struct.unpack('!Q', recvall(conn, 8))[0]))
    vol_id = req['volume_id']

    log('volume(%d)', vol_id)

    if vol_id not in G.volumes:
        path = os.path.join(G.volumes_dir, str(vol_id))
        G.volumes[vol_id] = os.open(path, os.O_CREAT | os.O_RDWR)
        os.lseek(G.volumes[vol_id], req['size'], os.SEEK_SET)
        os.write(G.volumes[vol_id], json.dumps(req).encode())

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
            with G.volume_lock:
                os.lseek(G.volumes[vol_id], offset, os.SEEK_SET)
                octets = os.read(G.volumes[vol_id], length)
                assert (len(octets) == length)

            with G.send_lock:
                conn.sendall(response_header)
                conn.sendall(octets)

        # WRITE - put the required data in the next batch
        # Backup thread would store the entire batch on the
        # cloud and then send the response back.
        if 1 == cmd:
            octets = recvall(conn, length)

            with G.batch_lock:
                G.batch.append((vol_id, offset, octets, conn, response_header))


def main(port, volumes_dir, config):
    G.volumes_dir = volumes_dir
    os.makedirs(G.volumes_dir, exist_ok=True)

    if 's3bucket' in config:
        G.s3 = S3Bucket(config['s3bucket'], config['s3bucket_auth_key'],
                        config['s3bucket_auth_secret'])

        G.log = json.loads(G.s3.get('log.json'))

        for lsn in range(G.log['applied']+1, G.log['total']+1):
            body = G.s3.get('logs/' + str(lsn))

            i = 0
            while i < len(body):
                vol_id, offset, length = struct.unpack('!QQQ', body[i:i+24])
                octets = body[i+24:i+24+length]
                i += 24 + length

                if vol_id not in G.volumes:
                    path = os.path.join(volumes_dir, str(vol_id))
                    G.volumes[vol_id] = os.open(path, os.O_CREAT | os.O_RDWR)

                os.lseek(G.volumes[vol_id], offset, os.SEEK_SET)
                os.write(G.volumes[vol_id], octets)

                log('lsn({}) volume({}) offset({}) length({})'.format(
                    lsn, vol_id, offset, length))

        for vol_id, fd in G.volumes.items():
            os.fsync(fd)

        G.log['applied'] = G.log['total']
        G.s3.put('log.json', json.dumps(G.log), 'application/json')

    threading.Thread(target=backup).start()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('', port))
    sock.listen(5)
    log('Listening on port(%d)', port)

    while True:
        conn, peer = sock.accept()
        threading.Thread(target=server, args=(conn,)).start()
        log('Connection accepted from %s', peer)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()

    ARGS.add_argument('--port', type=int, default='9876',
                      help='server port')
    ARGS.add_argument('--volumes', default='volumes',
                      help='Path to keep volume snapshots')
    ARGS.add_argument('--config', default='config.json',
                      help='Config file in json format')

    ARGS = ARGS.parse_args()

    with open(ARGS.config) as fd:
        ARGS.config = json.load(fd)

    main(ARGS.port, ARGS.volumes, ARGS.config)
