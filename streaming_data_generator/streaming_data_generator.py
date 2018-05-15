#!/usr/bin/env python
#
# Generate synthetic IIoT data and send to the REST gateway.
#
# Written by: claudio.fahey@dell.com
#

from __future__ import division
from __future__ import print_function
from time import sleep, time
import argparse
import requests
from multiprocessing import Process
import random
from math import sin, pi

def chunks(l, n):
    """Yield successive n-sized chunks from l.
    From https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks"""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def sine_wave_generator(temp_celsius_low, temp_celsius_high, temp_celsius_period_ms, temp_celsius_offset_ms, report_period_sec, device_id, t0_ms):
    t = t0_ms
    while True:
        temp_celsius_normalized = 0.5 * sin(2.0 * pi * (t - temp_celsius_offset_ms) / temp_celsius_period_ms) + 0.5
        temp_celsius = (temp_celsius_high - temp_celsius_low) * temp_celsius_normalized + temp_celsius_low
        data = {'timestamp': t, 'event_type': 'temp', 'device_id': device_id, 'temp_celsius': temp_celsius}
        yield data
        vibration1 = 10.0 * (temp_celsius + 273.15)
        vibration2 = 20.0 * (temp_celsius + 273.15) + 100.0
        data = {'timestamp': t, 'event_type': 'vibration', 'device_id': device_id, 'vibration1': vibration1, 'vibration2': vibration2}
        yield data
        t += int(1000.0 * report_period_sec)

def single_generator_process(options, device_id):
    url = options.gateway_url
    t0_ms = int(time() * 1000)

    # Create random parameters for this device.
    temp_celsius_low = random.uniform(0.0, 50.0)
    temp_celsius_high = random.uniform(50.0, 100.0)
    temp_celsius_period_ms = random.uniform(30000.0, 120000.0)
    temp_celsius_offset_ms = random.uniform(0.0, 1000000.0)
    report_period_sec = random.uniform(0.75, 1.5)
    print('%s: temp_celsius_low=%f, temp_celsius_high=%f, temp_celsius_period_ms=%f, temp_celsius_offset_ms=%f, report_period_sec=%f' %
          (device_id, temp_celsius_low, temp_celsius_high, temp_celsius_period_ms, temp_celsius_offset_ms, report_period_sec))
    generator = sine_wave_generator(temp_celsius_low, temp_celsius_high, temp_celsius_period_ms, temp_celsius_offset_ms, report_period_sec, device_id, t0_ms)

    for data in generator:
        try:
            t = data['timestamp']
            sleep_sec = t/1000.0 - time()
            if sleep_sec > 0.0:
                sleep(sleep_sec)
            print(device_id + ': ' + str(data))
            response = requests.post(url, json=data)
            print(device_id + ': ' + str(response))
            response.raise_for_status()
        except Exception as e:
            print(device_id + ': ERROR: ' + str(e))
            pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-g', '--gateway-url', default='http://localhost:3000/data',
        action='store', dest='gateway_url', help='URL for gateway')
    parser.add_argument(
        '--num-devices', default=1, type=int,
        action='store', dest='num_devices', help='Number of devices to simulate')
    options, unparsed = parser.parse_known_args()

    device_ids = ['%04d' % (i + 1) for i in range(options.num_devices)]
    while True:
        processes = [
            Process(target=single_generator_process, args=(options, device_id))
            for device_id in device_ids]
        [p.start() for p in processes]
        [p.join() for p in processes]

if __name__ == '__main__':
    main()
