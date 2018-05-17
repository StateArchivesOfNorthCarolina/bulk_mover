import os
from shutil import copyfile
from collections import OrderedDict
from pathlib import Path
import filetype
import re
from mover_classes.ConvertImgFile import ConvertImgFile
from mover_classes.ConvertAvFile import ConvertAudioFile
from mover_classes.ConvertAvFile import ConvertVideoFile
from mover_classes.ConvertDocumentFile import ConvertDocumentFile
from mover_classes.ConvertDocumentFile import ConvertNoAccessFile


class PathMunger:
    CONVERT_EXTS = {'.dv': 'video/x-dv',
                    '.mxf': 'video/mxf',
                    '.docx': 'sanc_document/msword',
                    '.doc': 'sanc_document/msword',
                    '.txt': 'sanc_document/plain',
                    '.pst': 'sanc_no_access/plain',
                    '.xlsx': None,
                    '.jpg': None,
                    '.jpeg': None
                    }

    IMAGE = 0
    AUDIO = 1
    VIDEO = 2
    DOCUMENT = 3
    NO_ACCESS = 4

    def __init__(self, source_base: str, dest_drive_letter: str) -> None:
        self.source_base = source_base  # Path without a /data
        self.source_base_no_drive = self._path_no_drive(source_base)  # Path without a drive letter
        self.source_mime = None  # mime type of the file
        self.source_file = None  # file_name with no path component
        self.source_current_full_path = None  # The full path to the file
        self.source_file_no_ext = None

        self.dest_drive_letter = dest_drive_letter
        self.dest_base_no_drive = None
        self.dest_file_path = None  # full final path with file
        self.dest_path = None  # The final path without a file component.
        self.dest_file = None  # The final destination filename whether changed or not.

        self.copy_error = None

        # Converters
        self.cvt_image = None  # type: ConvertImgFile
        self.cvt_audio = None  # type: ConvertAudioFile
        self.cvt_video = None  # type: ConvertVideoFile
        self.cvt_document = None  # type: ConvertDocumentFile
        self.cvt_no_access = None  # type: ConvertNoAccessFile
        self.current_type = None  # type: int
        self.is_no_access = False

    def set_current_targets(self, current_root, current_file):
        '''
        set_current_targets: does the heavy lifting of setting the current relative locations of the source and the destination

        :param current_root:
        :param current_file:
        :return:
        '''
        self.current_root = current_root
        self._set_dest_path(current_root)
        self.source_file = current_file
        self.source_current_full_path = os.path.join(current_root, current_file)
        self.source_file_no_ext = self.get_source_no_ext()

    def get_filename_no_ext(self):
        x = re.search('(\.[^.]*$|$)', self.source_file)
        y = x.regs[1]
        fn = self.source_file[0:y[0]]
        return fn

    def get_source_no_ext(self):
        return os.path.join(self.current_root, self.get_filename_no_ext())

    def is_pass_through(self) -> bool:
        if not self.is_known_type():
            # this is a pass through file no further processing involved
            self.dest_file_path = os.path.join(self.dest_path, self.source_file)
            self.dest_file = self.source_file
            return True
        else:
            return False

    def do_a_copy(self) -> bool:
        try:
            copyfile(self.source_current_full_path, self.dest_file_path)
            return True
        except OSError as e:
            self.copy_error = e
            return False
        except TypeError as e:
            self.copy_error = e
            return False

    @staticmethod
    def _path_no_drive(path: str) -> str:
        return os.path.splitdrive(path)[1]

    def get_source_bag(self) -> str:
        return os.path.join(self.source_base, "data")

    def get_source_base(self) -> str:
        return self.dest_drive_letter + self.source_base_no_drive

    def get_dest_base(self):
        return self.dest_drive_letter + self.dest_base_no_drive

    def _set_dest_path(self, source_root):
        l = OrderedDict.fromkeys(source_root.split(os.path.sep))
        r = OrderedDict.fromkeys(self.get_source_bag().split(os.path.sep))
        d = OrderedDict.fromkeys(x for x in l if x not in r)
        if len(d) == 0:
            self.dest_path = self.get_source_base()
        else:
            rel = os.path.sep.join([x for x in d])
            self.source_base = os.path.join(self.source_base, rel)
            self.dest_path = os.path.join(self.get_source_base(), rel)
        self.dest_base_no_drive = self._path_no_drive(self.dest_path)

    def get_dest_file_path(self):
        try:
            return os.path.join(self.dest_path, self.dest_file)
        except TypeError as e:
            return "PathError: {}".format(e)

    def get_source_file_path(self):
        return self.source_current_full_path

    def create_dest_path(self):
        if not os.path.exists(self.get_dest_base()):
            Path(self.get_dest_base()).mkdir(parents=True, exist_ok=False)

    def set_source_file(self, fname):
        self.source_file = fname

    def is_dest_there(self):
        if os.path.exists(self.get_dest_file_path()):
            return True
        return False

    def needs_conversion(self):
        if self.source_file == self.dest_file:
            return False
        return True

    def is_known_type(self):
        ext = os.path.splitext(self.source_file)[1].lower()

        if ext in PathMunger.CONVERT_EXTS:
            self.source_mime = PathMunger.CONVERT_EXTS[ext]
            if self.source_mime is None:
                return False
            return True
        else:
            source_mime = filetype.guess(self.source_current_full_path)
            if source_mime is None:
                return False
            self.source_mime = source_mime.mime
            return True

    def get_mime_type(self):
        if self.source_mime is not None:
            return self.source_mime.split("/")[0]

    def set_dest_file_name(self, ext: str):
        self.dest_file = self.get_file_no_ext() + ext
        self.dest_file_path = os.path.join(self.dest_path, self.dest_file)

    def get_dest_base_with_file_no_ext(self):
        return os.path.join(self.dest_path, self.get_file_no_ext())

    def get_file_no_ext(self):
        return self.get_filename_no_ext()

    def get_img_converter(self) -> ConvertImgFile:
        self.cvt_image = ConvertImgFile(self.source_current_full_path, self.source_mime)
        self.current_type = PathMunger.IMAGE
        return self.cvt_image

    def get_audio_converter(self) -> ConvertAudioFile:
        self.cvt_audio = ConvertAudioFile(self.source_current_full_path)
        self.current_type = PathMunger.AUDIO
        return self.cvt_audio

    def get_video_converter(self) -> ConvertVideoFile:
        self.cvt_video = ConvertVideoFile(self.source_current_full_path)
        self.current_type = PathMunger.VIDEO
        return self.cvt_video

    def get_document_converter(self) -> ConvertDocumentFile:
        self.cvt_document = ConvertDocumentFile(self.source_current_full_path)
        self.current_type = PathMunger.DOCUMENT
        return self.cvt_document

    def get_noaccess_converter(self) -> ConvertNoAccessFile:
        self.cvt_no_access = ConvertNoAccessFile(self.source_current_full_path)
        self.current_type = PathMunger.NO_ACCESS
        return self.cvt_no_access

    def get_success_message(self) -> tuple:
        info = "COPIED: \t{} \t--->\t {}".format(self.source_current_full_path, self.dest_file_path)
        log = "{}\t{}\n".format(self.get_source_file_path(), self.get_dest_file_path())
        return info, log

    def get_fail_message(self, err: str) -> tuple:
        info = "NOT COPIED: \t{} \t--->\t {}".format(self.source_current_full_path, err)
        log = "{}\t{}\t{}\n".format(self.source_current_full_path, self.dest_file_path, err)
        return info, log

    def get_error(self):
        if self.current_type == PathMunger.IMAGE:
            return self.cvt_image.error_msg
        if self.current_type == PathMunger.AUDIO:
            return self.cvt_audio.err_msg
        if self.current_type == PathMunger.VIDEO:
            return self.cvt_video.err_msg
        if self.current_type == PathMunger.DOCUMENT:
            return self.cvt_document.error
        if self.current_type == PathMunger.NO_ACCESS:
            return self.cvt_no_access.error