import os
import struct
import logging
import argparse
from logging import critical as log


def main(filepath, logdir):
    log_files = [int(p) for p in os.listdir(logdir) if p.isdigit()]

    with open(filepath, 'wb') as wfile:
        for filenum in sorted(log_files):
            with open(os.path.join(logdir, str(filenum)), 'rb') as rfile:
                while True:
                    octets = rfile.read(16)
                    if not octets:
                        break

                    offset, length = struct.unpack('!QQ', octets)
                    octets = rfile.read(length)

                    wfile.seek(offset)
                    wfile.write(octets)

                    log('file({}) offset({}) length({})'.format(filenum, offset, length))



if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    ARGS = argparse.ArgumentParser()
    ARGS.add_argument('--filepath', help='Snapshot file or device')
    ARGS.add_argument('--logdir', help='Log directory')
    ARGS = ARGS.parse_args()

    main(ARGS.filepath, ARGS.logdir)
