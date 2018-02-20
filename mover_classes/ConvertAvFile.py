import subprocess
import os


class ConvertAvFile(object):
    def __init__(self, av_in_file: str, av_root: str, av_out_file: str = None) -> None:
        self.av_in = av_in_file
        self.av_out = av_out_file
        self.a_root = av_root
        self.opts = None
        self.convert_list = []

    def convert(self, out_file: str):
        pass

    def needs_conversion(self, mime):
        if mime in self.convert_list:
            return False
        return True

    def get_out_file(self):
        pass


class ConvertAudioFile(ConvertAvFile):
    def __init__(self, av_in_file: str, av_out_file: str = None) -> None:
        super().__init__(av_in_file, av_out_file)
        self.convert_list = ["audio/m4a"]

    def convert(self, out_file: str):
        self.av_out = out_file
        if os.path.exists(out_file):
            return True
        if self.opts is None:
            self.opts = ['ffmpeg', '-i', self.av_in, out_file]

        if subprocess.call(self.opts) < 0:
            return False
        return True


class ConvertVideoFile(ConvertAvFile):
    def __init__(self, av_in_file: str, av_out_file: str = None) -> None:
        super().__init__(av_in_file, av_out_file)
        self.convert_list = ["video/mp4"]

    def convert(self, out_file: str):
        self.av_out = out_file
        if os.path.exists(out_file):
            return True
        if self.opts is None:
            self.opts = ['ffmpeg', '-i', self.av_in,
                         '-c:v', 'libx264', '-preset', 'slow', '-crf', '20',
                         '-c:a', 'aac', '-b:a', '384k',
                         out_file]
        if subprocess.call(self.opts) < 0:
            return False
        return True
