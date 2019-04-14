#!/usr/bin/env python
#
# Generate synthetic video data and send to the Pravega Gateway.
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
import grpc
import pravega
import logging
import zlib
import struct
from datetime import datetime, timedelta


def chunks(l, n):
    """Yield successive n-sized chunks from l.
    From https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks"""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def data_generator(camera, ssrc, frames_per_sec, max_chunks, max_chunk_data_size, t0_ms):
    frame_number = 0
    while True:
        timestamp = int(frame_number / (frames_per_sec / 1000.0) + t0_ms)
        data_size = np.random.randint(1, max_chunks * max_chunk_data_size - 4 + 1)
        data = np.random.bytes(data_size)
        # Add CRC32 checksum to allow for error checking.
        chucksum = struct.pack('!I', zlib.crc32(data))
        data = chucksum + data
        chunk_list = list(chunks(data, max_chunk_data_size))
        num_chunks = len(chunk_list)
        if num_chunks > max_chunks:
            raise Exception('num_chunks exceeded max_chunks')
        for chunk in range(max_chunks):
            # To accommodate the reassembly algorithm in Spark, we must write records for all chunks including empty ones.
            if chunk < num_chunks:
                chunk_data = chunk_list[chunk]
            else:
                chunk_data = b''
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


def pravega_request_generator(data_generator, scope, stream):
    for record in data_generator:
        t = record['timestamp']
        sleep_sec = t/1000.0 - time()
        if sleep_sec > 0.0:
            sleep(sleep_sec)
        record['timestamp'] = (datetime(1970, 1, 1) + timedelta(milliseconds=t)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
        data_json = json.dumps(record)
        if True:
            record_to_log = record.copy()
            record_to_log['data'] = record_to_log['data'][:10] + '...'
            record_to_log['data_len'] = len(record['data'])
            record_to_log['json_len'] = len(data_json)
            logging.info('%d: %s' % (record_to_log['camera'], json.dumps(record_to_log)))
        request = pravega.pb.WriteEventsRequest(scope=scope, stream=stream, event=data_json.encode('utf-8'))
        yield request


def single_generator_process(camera, frames_per_sec, max_chunks, max_chunk_data_size, gateway, scope, stream):
    t0_ms = int(time() * 1000)
    ssrc = np.random.randint(0, 2**31)
    data_iter = data_generator(camera, ssrc, frames_per_sec, max_chunks, max_chunk_data_size, t0_ms)
    # data_iter = itertools.islice(data_iter, 10)
    pravega_request_iter = pravega_request_generator(data_iter, scope, stream)
    with grpc.insecure_channel(gateway) as pravega_channel:
        pravega_client = pravega.grpc.PravegaGatewayStub(pravega_channel)
        pravega_client.CreateScope(pravega.pb.CreateScopeRequest(scope=scope))
        pravega_client.CreateStream(pravega.pb.CreateStreamRequest(scope=scope, stream=stream))
        write_response = pravega_client.WriteEvents(pravega_request_iter)
        logging.info("write_response=" + str(write_response))


def main():
    logging.basicConfig(level=logging.DEBUG)
    parser = argparse.ArgumentParser(auto_env_var_prefix='GENERATOR_')
    parser.add_argument(
        '-g', '--gateway', default='localhost:50051',
        action='store', dest='gateway', help='address:port of Pravega gateway')
    parser.add_argument(
        '--scope', default='examples4',
        action='store', dest='scope', help='Pravega scope')
    parser.add_argument(
        '--stream', default='video',
        action='store', dest='stream', help='Pravega stream')
    parser.add_argument(
        '--num-cameras', default=1, type=int,
        action='store', dest='num_cameras', help='Number of cameras to simulate')
    parser.add_argument(
        '--max-chunks', default=3, type=int,
        action='store', dest='max_chunks', help='Maximum number of chunks per frame')
    parser.add_argument(
        '--max-chunk-data-size', default=10, type=int,
        action='store', dest='max_chunk_data_size', help='Maximum size of data per chunk (bytes)')
    parser.add_argument(
        '--fps', default=5.0, type=float,
        action='store', dest='frames_per_sec', help='Number of frames per second per camera')
    options, unparsed = parser.parse_known_args()

    cameras = [i for i in range(options.num_cameras)]
    if True:
        processes = [
            Process(
                target=single_generator_process,
                args=(
                    camera,
                    options.frames_per_sec,
                    options.max_chunks,
                    options.max_chunk_data_size,
                    options.gateway,
                    options.scope,
                    options.stream))
            for camera in cameras]
        [p.start() for p in processes]
        [p.join() for p in processes]


if __name__ == '__main__':
    main()
