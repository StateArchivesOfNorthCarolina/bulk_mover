from PIL import Image

Image.MAX_IMAGE_PIXELS = 1000000000
from io import BytesIO
import os
import tempfile


class ConvertImgError(Exception):

    def __init__(self, msg) -> None:
        self.message = str(msg)


class ConvertImgFile(object):
    def __init__(self, img_in_file: str, mime: str, img_out: str = None) -> None:
        self.img_in = img_in_file
        self.img_out = img_out
        self.mime = mime
        self.opened_image = None    # type: Image
        self.error_msg = None       # type: str
        self.ext_map = {"image/tiff": '.jpg',
                        "image/tif": '.jpg',
                        "image/vnd.adobe.photoshop": '.png',
                        "image/png": ".jpg",
                        "image/gif": ".jpg",
                        "image/webp": ".jpg",
                        "image/cr2": ".jpg",
                        "image/bmp": ".jpg",
                        "image/jxr": ".jpg"}

        self.current_converted_file = None
        self.a_root = None
        self.p_root = None

    def needs_conversion(self):
        if self.mime not in self.ext_map.keys():
            return False
        return True

    def _open_image(self) -> bool:
        try:
            self.opened_image = Image.open(self.img_in)
        except IOError as e:
            self.error_msg = e
            return False
        return True

    def convert(self) -> bool:
        self._open_image()
        try:
            self.opened_image.save(self.img_out)
            self.opened_image.close()
            return True
        except IOError as e:
            if self.convert_multilayer_tiff():
                return True
            self.error_msg = e
            return False
        except AttributeError as e:
            self.error_msg = e
            return False

    def convert_multilayer_tiff(self):
        try:
            self.opened_image.mode = 'I'
            self.opened_image.point(lambda i: i*(1./256)).convert('L').save(self.img_out)
            self.opened_image.close()
            return True
        except Exception as e:
            return False

    def which_ext(self):
        try:
            return self.ext_map[self.mime]
        except KeyError as e:
            self.error_msg = e
            raise ConvertImgError(e)

