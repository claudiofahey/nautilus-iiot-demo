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
import grpc
import logging
import struct
import math
from datetime import datetime, timedelta
import pravega
from PIL import Image, ImageDraw, ImageFont
import io


def chunks(l, n):
    """Yield successive n-sized chunks from l.
    From https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks"""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def pravega_chunker_v1(payload, max_chunk_size):
    """Split payload into chunks of 1 MiB or less.
    When written to Pravega, chunked events have the following 64-bit header.
      version: value must be 1 (8-bit signed integer)
      reserved: value must be 0 (3 8-bit signed integers)
      chunk_index: 0-based chunk index (16-bit signed big endian integer)
      final_chunk_index: number of chunks minus 1 (16 bit signed big endian integer)
    """
    version = 1
    max_chunk_data_size = max_chunk_size - 4096
    chunk_list = list(chunks(payload, max_chunk_data_size))
    final_chunk_index = len(chunk_list) - 1
    for chunk_index, chunked_payload in enumerate(chunk_list):
        is_final_chunk = chunk_index == final_chunk_index
        header = struct.pack('!bxxxhh', version, chunk_index, final_chunk_index)
        chunked_event = header + chunked_payload
        yield (chunked_event, chunk_index, is_final_chunk)


def pravega_chunker_selector(payload, max_chunk_size, chunk_version):
    if chunk_version == 1:
        return pravega_chunker_v1(payload, max_chunk_size)
    else:
        return [(payload, 0, True)]


def build_image_bytes(num_bytes, camera, frame_number, timestamp_ms):
    timestamp_str = (datetime(1970, 1, 1) + timedelta(milliseconds=timestamp_ms)).strftime("%H:%M:%S.%f")[:-3]
    width = math.ceil(math.sqrt(num_bytes / 3))
    height = width
    font_size = int(min(width, height) * 0.13)
    img = Image.new('RGB', (width, height), (25, 25, 240, 0))
    font = ImageFont.truetype('/usr/share/fonts/truetype/freefont/FreeMonoBold.ttf', font_size)
    draw = ImageDraw.Draw(img)
    draw.text(
        (5, 5),
        'CAMERA\n %04d\nFRAME\n%05d\n%s' % (camera, frame_number, timestamp_str),
        font=font,
        align='center')
    out_bytesio = io.BytesIO()
    img.save(out_bytesio, format='PNG', compress_level=0)
    out_bytes = out_bytesio.getvalue()
    return out_bytes


def data_generator(camera, ssrc, frames_per_sec, avg_data_size, include_checksum, t0_ms):
    frame_number = 0
    while True:
        timestamp = int(frame_number / (frames_per_sec / 1000.0) + t0_ms)
        num_bytes = avg_data_size
        data = build_image_bytes(num_bytes, camera, frame_number, timestamp)
        if frame_number == 0:
            with open('/tmp/camera%d-%d.png' % (camera, frame_number), 'wb') as f:
                f.write(data)
        record = {
            'timestamp': timestamp,
            'frame_number': frame_number,
            'camera': camera,
            'ssrc': ssrc,
            'data': data,
        }
        yield record
        frame_number += 1


def prepare_encode_record(record: dict) -> dict:
    """Convert record data types to JSON-serializable types.
    JSON is universally compatible but require images to be base64 encoded.
    For optimal performance, other encodings should be used such as Avro or Protobuf.
    """
    r = record.copy()
    r['data'] = base64.b64encode(record['data']).decode('utf-8')
    r['timestamp'] = (datetime(1970, 1, 1) + timedelta(milliseconds=r['timestamp'])).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
    return r


def encode_record(prepared_record: dict) -> bytes:
    encoded = json.dumps(prepared_record).encode('utf-8')
    return encoded


def pravega_request_generator(data_generator, scope, stream, max_chunk_size, use_transactions, chunk_version, commit_period):
    for record in data_generator:
        t = record['timestamp']
        prepared_record = prepare_encode_record(record)
        payload = encode_record(prepared_record)
        sleep_sec = t/1000.0 - time()
        if sleep_sec > 0.0:
            sleep(sleep_sec)
        elif sleep_sec < -5.0:
            logging.warn(f"Data generator can't keep up with real-time. sleep_sec={sleep_sec}")
        for (chunked_event, chunk_index, is_final_chunk) in pravega_chunker_selector(payload, max_chunk_size=max_chunk_size, chunk_version=chunk_version):
            request = pravega.pb.WriteEventsRequest(
                event=chunked_event,
                scope=scope,
                stream=stream,
                use_transaction=use_transactions,
                commit=is_final_chunk and use_transactions and (record['frame_number'] % commit_period == 0),
                )
            # logging.info(request)
            yield request
        if True:
            record_to_log = record.copy()
            # record_to_log['data'] = str(record_to_log['data'][:10]) + '...'
            del record_to_log['data']
            record_to_log['data_len'] = len(record['data'])
            record_to_log['payload_len'] = len(payload)
            record_to_log['final_chunk_index'] = chunk_index
            record_to_log['timestamp'] = prepared_record['timestamp']
            logging.info('%d: %s' % (record_to_log['camera'], json.dumps(record_to_log)))


def single_generator_process(camera, frames_per_sec, max_chunk_size, avg_data_size, checksum, gateway, scope, stream, use_transactions, chunk_version, commit_period):
    t0_ms = int(time() * 1000)
    ssrc = np.random.randint(0, 2**31)
    data_iter = data_generator(camera, ssrc, frames_per_sec, avg_data_size, checksum, t0_ms)
    pravega_request_iter = pravega_request_generator(data_iter, scope, stream, max_chunk_size, use_transactions, chunk_version, commit_period)
    with grpc.insecure_channel(gateway) as pravega_channel:
        pravega_client = pravega.grpc.PravegaGatewayStub(pravega_channel)
        pravega_client.CreateScope(pravega.pb.CreateScopeRequest(scope=scope))
        pravega_client.CreateStream(pravega.pb.CreateStreamRequest(scope=scope, stream=stream))
        pravega_client.WriteEvents(pravega_request_iter)


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
        '--stream', default='unchunkedvideo',
        action='store', dest='stream', help='Pravega stream')
    parser.add_argument(
        '--num-cameras', default=4, type=int,
        action='store', dest='num_cameras', help='Number of cameras to simulate')
    parser.add_argument(
        '--max-chunk-size', default=1024*1024, type=int,
        action='store', dest='max_chunk_size', help='Maximum size of chunk (bytes)')
    parser.add_argument(
        '--avg-data-size', default=1*1024, type=int,
        action='store', dest='avg_data_size', help='Average size of data (bytes)')
    parser.add_argument(
        '--fps', default=2.0, type=float,
        action='store', dest='frames_per_sec', help='Number of frames per second per camera')
    parser.add_argument(
        '--checksum', default=False,
        action='store_true', dest='checksum', help='Prepend the data with a checksum that can be used to detect errors')
    parser.add_argument(
        '--use-transactions', default=True,
        action='store_true', dest='use_transactions', help='If true, use Pravega transactions')
    parser.add_argument(
        '--chunk-version', default=0, type=int,
        action='store', dest='chunk_version', help='0=no chunking, 1=use chunking')
    parser.add_argument(
        '--commit-period', default=8, type=int,
        action='store', dest='commit_period', help='Transactions will be committed after this many frames')
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
                    options.avg_data_size,
                    options.checksum,
                    options.gateway,
                    options.scope,
                    options.stream,
                    options.use_transactions,
                    options.chunk_version,
                    options.commit_period,
                ))
            for camera in cameras]
        [p.start() for p in processes]
        [p.join() for p in processes]


if __name__ == '__main__':
    main()
