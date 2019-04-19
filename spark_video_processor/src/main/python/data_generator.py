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


def pravega_chunker_v1(payload, max_chunk_size=1024*1024):
    """Split payload into chunks of 1 MiB or less.
    When written to Pravega, chunked events have the following 64-bit header.
      version: value must be 1 (8-bit signed integer)
      reserved: value must be 0 (3 8-bit signed integers)
      chunk_index: 0-based chunk index (16-bit signed big endian integer)
      final_chunk_index: number of chunks minus 1 (16 bit signed big endian integer)
    """
    version = 1
    reserved = 0
    max_chunk_data_size = max_chunk_size - 4096
    chunk_list = list(chunks(payload, max_chunk_data_size))
    final_chunk_index = len(chunk_list) - 1
    for chunk_index, chunked_payload in enumerate(chunk_list):
        header = struct.pack('!bxxxhh', version, chunk_index, final_chunk_index)
        chunked_event = header + chunked_payload
        yield chunked_event


def data_generator(camera, ssrc, frames_per_sec, t0_ms):
    frame_number = 0
    while True:
        timestamp = int(frame_number / (frames_per_sec / 1000.0) + t0_ms)
        # data_size = np.random.randint(1, max_chunks * max_chunk_size - 4 + 1)
        data_size = 20
        data = np.random.bytes(data_size)
        # Add CRC32 checksum to allow for error checking.
        chucksum = struct.pack('!I', zlib.crc32(data))
        data = chucksum + data
        record = {
            'timestamp': timestamp,
            'frame_number': frame_number,
            'camera': camera,
            'ssrc': ssrc,
            'data': base64.b64encode(data).decode('utf-8'),
        }
        yield record
        frame_number += 1


def pravega_request_generator(data_generator, scope, stream, max_chunk_size):
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
        payload = data_json.encode('utf-8')
        for chunked_event in pravega_chunker_v1(payload, max_chunk_size=max_chunk_size):
            request = pravega.pb.WriteEventsRequest(
                scope=scope,
                stream=stream,
                use_transaction=False,
                event=chunked_event)
            logging.info(request)
            yield request


def single_generator_process(camera, frames_per_sec, max_chunk_size, gateway, scope, stream):
    t0_ms = int(time() * 1000)
    ssrc = np.random.randint(0, 2**31)
    data_iter = data_generator(camera, ssrc, frames_per_sec, t0_ms)
    # data_iter = itertools.islice(data_iter, 10)
    pravega_request_iter = pravega_request_generator(data_iter, scope, stream, max_chunk_size)
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
        '-g', '--gateway', default='localhost:54672',
        action='store', dest='gateway', help='address:port of Pravega gateway')
    parser.add_argument(
        '--scope', default='examples',
        action='store', dest='scope', help='Pravega scope')
    parser.add_argument(
        '--stream', default='video',
        action='store', dest='stream', help='Pravega stream')
    parser.add_argument(
        '--num-cameras', default=1, type=int,
        action='store', dest='num_cameras', help='Number of cameras to simulate')
    # parser.add_argument(
    #     '--max-chunks', default=3, type=int,
    #     action='store', dest='max_chunks', help='Maximum number of chunks per frame')
    parser.add_argument(
        '--max-chunk-size', default=4096+10, type=int,
        action='store', dest='max_chunk_size', help='Maximum size of chunk (bytes)')
    parser.add_argument(
        '--fps', default=1.0, type=float,
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
                    options.max_chunk_size,
                    options.gateway,
                    options.scope,
                    options.stream))
            for camera in cameras]
        [p.start() for p in processes]
        [p.join() for p in processes]


if __name__ == '__main__':
    main()
