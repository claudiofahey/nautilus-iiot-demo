#!/usr/bin/env python

from PIL import Image, ImageDraw, ImageFont
import io
import math


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


def main():
    # rgb = np.random.randint(255, size=(5,5,3), dtype=np.uint8)
    # # cv2.imwrite("/tmp/test.png", rgb)
    # _, png_np = cv2.imencode('.png', rgb, [cv2.IMWRITE_PNG_COMPRESSION, 0])
    # print(png_np)
    # in_png_bytes = bytes(png_np)
    # print(in_png_bytes)
    # print(png_np)
    # rgb2 = cv2.imdecode(png_np, -1)
    # print(rgb2)
    # print(rgb2.mean())

    out_bytes = build_image_bytes(num_bytes=300*1000, camera=203, frame_number=99999)
    # draw.text((font_size, font_size), 'frame %05d' % frame_number, font=font, align='center')

    # in_pil = Image.open(io.BytesIO(in_png_bytes))

    # out_pil = Image.new('RGB', (10, 5))
    # out_pil.paste(in_pil, (0, 0))
    # out_pil.paste(in_pil, (5, 0))

    # out_pil = in_pil
    #
    # out_bytesio = io.BytesIO()
    # out_pil.save(out_bytesio, format='PNG')
    # out_bytes = out_bytesio.getvalue()

    # print(out_bytes)

    with open('/tmp/image.png', 'wb') as f:
        f.write(out_bytes)


if __name__ == '__main__':
    main()
