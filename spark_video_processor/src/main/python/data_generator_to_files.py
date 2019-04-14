#!/usr/bin/env python
#
# Generate synthetic video data as JSON files.
#
# Written by: claudio.fahey@dell.com
#

from time import sleep, time
import configargparse as argparse
from multiprocessing import Process
import json
import numpy as np
import base64
import math
import os
import itertools


def chunks(l, n):
    """Yield successive n-sized chunks from l.
    From https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks"""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def data_generator(camera, ssrc, frames_per_sec, num_chunks, t0_ms):
    frame_number = 0
    while True:
        timestamp = int(frame_number / (frames_per_sec / 1000.0) + t0_ms)
        data = np.random.bytes(100)
        for chunk, chunk_data in enumerate(chunks(data, math.ceil(len(data)/num_chunks))):
            record = {
                'timestamp': timestamp,
                'frame_number': frame_number,
                'camera': camera,
                'chunk': chunk,
                'num_chunks': num_chunks,
                'ssrc': ssrc,
                'data': base64.b64encode(chunk_data).decode('utf-8'),
            }
            yield record
        frame_number += 1


def single_generator_process(camera, ssrc, frames_per_sec, num_chunks, output_dir):
    t0_ms = int(time() * 1000)
    generator = data_generator(camera, ssrc, frames_per_sec, num_chunks, t0_ms)
    generator = itertools.islice(generator, 10)
    out_file_name = os.path.join(output_dir, 'camera%d.json' % camera)
    with open(out_file_name, 'w') as out_file:
        for record in generator:
            try:
                line = json.dumps(record)
                print('%d: %s' % (camera, line))
                out_file.write(line)
                out_file.write('\n')
            except Exception as e:
                print('%s: ERROR: %s' % (camera, e))
                pass


def main():
    parser = argparse.ArgumentParser(auto_env_var_prefix='GENERATOR_')
    # parser.add_argument(
    #     '-g', '--gateway-url', default='http://localhost:3000/data',
    #     action='store', dest='gateway_url', help='URL for gateway')
    parser.add_argument(
        '--num-cameras', default=1, type=int,
        action='store', dest='num_cameras', help='Number of cameras to simulate')
    parser.add_argument(
        '--output-dir', default='/tmp/camera_data',
        action='store', dest='output_dir', help='')
    options, unparsed = parser.parse_known_args()

    cameras = [i for i in range(options.num_cameras)]
    ssrc = 0
    frames_per_sec = 30
    num_chunks = 3
    if True:
        processes = [
            Process(target=single_generator_process, args=(camera, ssrc, frames_per_sec, num_chunks, options.output_dir))
            for camera in cameras]
        [p.start() for p in processes]
        [p.join() for p in processes]


if __name__ == '__main__':
    main()
