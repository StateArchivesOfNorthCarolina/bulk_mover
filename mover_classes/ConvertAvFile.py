import subprocess
import os


class ConvertAvFile(object):
    def __init__(self, av_in_file: str, av_root: str, av_out_file: str = None) -> None:
        self.av_in = av_in_file
        self.av_out = av_out_file
        self.a_root = av_root
        self.mime = None
        self.opts = None
        self.ext_map = {"video/mp4": '.m4v',
                        "video/x-matroska": ".m4v",
                        "video/webm": ".m4v",
                        "video/quicktime": ".m4v",
                        "video/x-msvideo": ".m4v",
                        "video/x-ms-wmv": ".m4v",
                        "video/mpeg": ".m4v",
                        "video/x-flv": ".m4v",
                        "audio/midi": ".m4a",
                        "audio/mpeg": ".m4a",
                        "audio/oog": ".m4a",
                        "audio/x-flac": ".m4a",
                        "audio/x-wav": ".m4a",
                        "audio/amr": ".m4a",
                        "video/x-dv": "m4v",
                        "video/mxf": "m4v"}
        self.err_msg = None

    def convert(self):
        pass

    def needs_conversion(self):
        if self.mime not in self.ext_map.keys():
            return False
        return True

    def which_ext(self):
        try:
            return self.ext_map[self.mime]
        except KeyError as e:
            print(self.av_in + " " + self.mime)
            raise KeyError

    def get_out_file(self):
        pass


class ConvertAudioFile(ConvertAvFile):
    def __init__(self, av_in_file: str, av_out_file: str = None) -> None:
        super().__init__(av_in_file, av_out_file)

    def convert(self):
        if self.opts is None:
            self.opts = ['C:\\Program Files\\ffmpeg\\bin\\ffmpeg.exe', '-i', self.av_in, self.av_out]
        try:
            cp = subprocess.run(self.opts)
            if cp.returncode == 0:
                return True
        except subprocess.SubprocessError as e:
            self.err_msg = e
        return False


class ConvertVideoFile(ConvertAvFile):
    def __init__(self, av_in_file: str, av_out_file: str = None) -> None:
        super().__init__(av_in_file, av_out_file)

    def convert(self):
        if self.opts is None:
            self.opts = ['C:\\Program Files\\ffmpeg\\bin\\ffmpeg.exe', '-i', self.av_in,
                         '-c:v', 'libx264', '-preset', 'slow', '-crf', '20',
                         '-c:a', 'aac', '-b:a', '384k',
                         self.av_out]
        try:
            cp = subprocess.run(self.opts)
            if cp.returncode == 0:
                return True
        except subprocess.SubprocessError as e:
            self.err_msg = "Could not convert: {}".format(e)
            return False
        except TypeError as e:
            self.err_msg = e
            return False
        return True
