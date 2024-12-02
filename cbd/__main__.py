import os
from cbd import cbd
import fcntl
import logging
import argparse
import threading


logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

ARGS = argparse.ArgumentParser()

ARGS.add_argument('--device',
    help='Network Block Device path')
ARGS.add_argument('--block_size', type=int, default=4096,
    help='Device Block Size')
ARGS.add_argument('--block_count', type=int, default=256*1024,
    help='Device Block Count')
ARGS.add_argument('--timeout', type=int, default=60,
    help='Timeout in seconds')
ARGS.add_argument('--socket_path',
    help='Socket path for communication')
ARGS.add_argument('--snapshot', default='snapshot',
    help='File for keeping a snapshot')

ARGS = ARGS.parse_args()

fcntl.flock(
    os.open(ARGS.snapshot, os.O_RDONLY),
    fcntl.LOCK_EX)

threading.Thread(
    target=cbd.server_thread,
    args=(ARGS.socket_path, ARGS.snapshot)).start()

threading.Thread(
    target=cbd.logger_thread,
    args=()).start()

cbd.device_init(
    ARGS.device, ARGS.block_size, ARGS.block_count,
    ARGS.timeout, ARGS.socket_path)
