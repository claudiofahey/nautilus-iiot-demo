#!/usr/bin/env python
#
# Generate synthetic non-video sensor data and send to the Pravega Gateway.
#
# Written by: claudio.fahey@dell.com
#

from time import sleep, time
import configargparse as argparse
from multiprocessing import Process
import json
import numpy as np
import grpc
import logging
import random
from datetime import datetime, timedelta
from math import sin, pi
import pravega


def data_generator(temp_celsius_low, temp_celsius_high, temp_celsius_period_ms, temp_celsius_offset_ms, report_period_sec, device_id, t0_ms):
    t = t0_ms
    while True:
        temp_celsius_normalized = 0.5 * sin(2.0 * pi * (t - temp_celsius_offset_ms) / temp_celsius_period_ms) + 0.5
        temp_celsius = (temp_celsius_high - temp_celsius_low) * temp_celsius_normalized + temp_celsius_low
        data = {'timestamp': t, 'event_type': 'temp', 'device_id': device_id, 'temp_celsius': temp_celsius}
        yield data
        if False:
            vibration1 = 10.0 * (temp_celsius + 273.15)
            vibration2 = 20.0 * (temp_celsius + 273.15) + 100.0
            data = {'timestamp': t, 'event_type': 'vibration', 'device_id': device_id, 'vibration1': vibration1, 'vibration2': vibration2}
            yield data
        t += int(1000.0 * report_period_sec)


def pravega_request_generator(data_generator, scope, stream):
    for record in data_generator:
        t = record['timestamp']
        sleep_sec = t/1000.0 - time()
        if sleep_sec > 0.0:
            sleep(sleep_sec)
        record['timestamp'] = (datetime(1970, 1, 1) + timedelta(milliseconds=t)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
        data_json = json.dumps(record)
        payload = data_json.encode('utf-8')
        request = pravega.pb.WriteEventsRequest(
            event=payload,
            scope=scope,
            stream=stream,
            )
        yield request
        if True:
            logging.info('%s: %s' % (record['device_id'], data_json))


def single_generator_process(device_id, gateway, scope, stream):
    t0_ms = int(time() * 1000)

    # Create random parameters for this device.
    temp_celsius_low = random.uniform(0.0, 50.0)
    temp_celsius_high = random.uniform(50.0, 100.0)
    temp_celsius_period_ms = random.uniform(120000.0, 600000.0)
    temp_celsius_offset_ms = random.uniform(0.0, 1000000.0)
    report_period_sec = random.uniform(0.75, 1.5)
    logging.info('%s: temp_celsius_low=%f, temp_celsius_high=%f, temp_celsius_period_ms=%f, temp_celsius_offset_ms=%f, report_period_sec=%f' %
          (device_id, temp_celsius_low, temp_celsius_high, temp_celsius_period_ms, temp_celsius_offset_ms, report_period_sec))

    data_iter = data_generator(temp_celsius_low, temp_celsius_high, temp_celsius_period_ms, temp_celsius_offset_ms, report_period_sec, device_id, t0_ms)
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
        '-g', '--gateway', default='localhost:54672',
        action='store', dest='gateway', help='address:port of Pravega gateway')
    parser.add_argument(
        '--scope', default='examples',
        action='store', dest='scope', help='Pravega scope')
    parser.add_argument(
        '--stream', default='sensors',
        action='store', dest='stream', help='Pravega stream')
    parser.add_argument(
        '--num-devices', default=2, type=int,
        action='store', dest='num_devices', help='Number of devices to simulate')
    options, unparsed = parser.parse_known_args()

    device_ids = ['%04d' % (i + 1) for i in range(options.num_devices)]
    if True:
        processes = [
            Process(
                target=single_generator_process,
                args=(
                    device_id,
                    options.gateway,
                    options.scope,
                    options.stream))
            for device_id in device_ids]
        [p.start() for p in processes]
        [p.join() for p in processes]


if __name__ == '__main__':
    main()
