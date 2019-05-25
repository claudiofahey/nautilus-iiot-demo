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


def build_image_bytes(num_bytes, camera, frame_number):
    width = math.ceil(math.sqrt(num_bytes / 3))
    height = width
    font_size = int(min(width, height) * 0.15)
    img = Image.new('RGB', (width, height), (25, 25, 240, 0))
    font = ImageFont.truetype('/usr/share/fonts/truetype/freefont/FreeSans.ttf', font_size)
    draw = ImageDraw.Draw(img)
    draw.text((width//10, height//10), 'FRAME\n%05d\nCAMERA\n %03d' % (frame_number, camera), font=font, align='center')
    out_bytesio = io.BytesIO()
    img.save(out_bytesio, format='PNG', compress_level=0)
    out_bytes = out_bytesio.getvalue()
    return out_bytes


def data_generator(camera, ssrc, frames_per_sec, avg_data_size, include_checksum, t0_ms):
    frame_number = 0
    while True:
        timestamp = int(frame_number / (frames_per_sec / 1000.0) + t0_ms)
        num_bytes = avg_data_size
        data = build_image_bytes(num_bytes, camera, frame_number)
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


def encode_record(record: dict) -> bytes:
    """Encode the record
    JSON is universally compatible but require images to be base64 encoded.
    For optimal performance, other encodings should be used such as Avro or Protobuf.
    """
    r = record.copy()
    r['data'] = base64.b64encode(record['data']).decode('utf-8')
    encoded = json.dumps(r).encode('utf-8')
    return encoded


def pravega_request_generator(data_generator, scope, stream, max_chunk_size, use_transactions):
    for record in data_generator:
        t = record['timestamp']
        sleep_sec = t/1000.0 - time()
        if sleep_sec > 0.0:
            sleep(sleep_sec)
        record['timestamp'] = (datetime(1970, 1, 1) + timedelta(milliseconds=t)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
        payload = encode_record(record)
        for (chunked_event, chunk_index, is_final_chunk) in pravega_chunker_v1(payload, max_chunk_size=max_chunk_size):
            request = pravega.pb.WriteEventsRequest(
                event=chunked_event,
                scope=scope,
                stream=stream,
                use_transaction=use_transactions,
                commit=is_final_chunk and use_transactions,
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
            logging.info('%d: %s' % (record_to_log['camera'], json.dumps(record_to_log)))


def single_generator_process(camera, frames_per_sec, max_chunk_size, avg_data_size, checksum, gateway, scope, stream, use_transactions):
    t0_ms = int(time() * 1000)
    ssrc = np.random.randint(0, 2**31)
    data_iter = data_generator(camera, ssrc, frames_per_sec, avg_data_size, checksum, t0_ms)
    pravega_request_iter = pravega_request_generator(data_iter, scope, stream, max_chunk_size, use_transactions)
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
        '--num-cameras', default=4, type=int,
        action='store', dest='num_cameras', help='Number of cameras to simulate')
    parser.add_argument(
        '--max-chunk-size', default=1024*1024, type=int,
        action='store', dest='max_chunk_size', help='Maximum size of chunk (bytes)')
    parser.add_argument(
        '--avg-data-size', default=10*1024, type=int,
        action='store', dest='avg_data_size', help='Average size of data (bytes)')
    parser.add_argument(
        '--fps', default=1.0, type=float,
        action='store', dest='frames_per_sec', help='Number of frames per second per camera')
    parser.add_argument(
        '--checksum', default=False,
        action='store_true', dest='checksum', help='Prepend the data with a checksum that can be used to detect errors')
    parser.add_argument(
        '--use-transactions', default=False,
        action='store_true', dest='use_transactions', help='If true, use Pravega transactions')
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
                ))
            for camera in cameras]
        [p.start() for p in processes]
        [p.join() for p in processes]


if __name__ == '__main__':
    main()
