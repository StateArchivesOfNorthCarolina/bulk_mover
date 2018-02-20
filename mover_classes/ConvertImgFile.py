from PIL import Image
from io import BytesIO
import os
import tempfile


class ConvertImgFile(object):
    def __init__(self, img_in_file: str, mime: str, img_out: str = None) -> None:
        self.img_in = img_in_file
        self.img_out = img_out
        self.mime = mime
        self.opened_image = None    # type: Image
        self.error_msg = None       # type: str
        self.convert_list = ["image/tiff"]
        self.current_converted_file = None
        self.a_root = None
        self.p_root = None

    def needs_conversion(self):
        if self.mime not in self.convert_list:
            return False
        return True

    def _open_image(self) -> bool:
        try:
            self.opened_image = Image.open(self.img_in)
        except IOError as e:
            self.error_msg = e
            return False
        return True

    def convert(self, out_file) -> bool:
        self._open_image()
        self.img_out = out_file
        if os.path.exists(out_file):
            return True
        try:
            self.opened_image.save(out_file)
            self.opened_image.close()
        except IOError as e:
            self.error_msg = e
            return False
        return True

