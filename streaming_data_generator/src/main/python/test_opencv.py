#!/usr/bin/env python

import numpy as np
import cv2


def main():
    rgb = np.random.randint(255, size=(5,5,3), dtype=np.uint8)
    # cv2.imwrite("/tmp/test.png", rgb)
    _, png_bytes = cv2.imencode('.png', rgb, [cv2.IMWRITE_PNG_COMPRESSION, 0])
    # print(png_bytes)
    rgb2 = cv2.imdecode(png_bytes, -1)
    print(rgb2)
    print(rgb2.mean())


if __name__ == '__main__':
    main()
