import logging
import argparse
from cbd import cbd


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
    '--volume', default='volume',
    help='File for keeping a volume')

ARGS.add_argument(
    '--s3path',
    help='s3path for this volume')

ARGS = ARGS.parse_args()

cbd.main(ARGS.device, ARGS.block_size, ARGS.block_count,
         ARGS.timeout, ARGS.volume, ARGS.s3path)
