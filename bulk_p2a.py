import os
from shutil import copyfile
from shutil import copyfileobj
import yaml
import logging
import logging.config
import filetype
from mover_classes.ConvertImgFile import ConvertImgFile
from mover_classes.ConvertAvFile import ConvertAudioFile
from mover_classes.ConvertAvFile import ConvertVideoFile
from pathlib import Path
from mover_classes.ConvertAvFile import ConvertAvFile
from tqdm import tqdm

class MoveInfo:

    def __init__(self, finfo: tuple) -> None:
        self.p_file = finfo[0]
        self.a_root = finfo[1]
        self.a_file = finfo[2]
        self.original_loc = finfo[3]
        self.mime = finfo[4]
        self.file = finfo[5]
        self.p_root = finfo[6]


class RootConvert:
    def __init__(self, inf: str, ext: str=None) -> None:
        self.in_file = inf
        self.in_file_name = os.path.basename(self.in_file)
        self.out_file_name = None
        self.out_file = None
        self.ext = ext
        self.l_base = None
        self.r_base = None
        self._get_bases()
        self.convert()

    def _get_bases(self):
        # get the dest base
        bp = os.path.dirname(self.in_file)
        _, in_base = os.path.splitdrive(bp)
        bases = in_base.split("data")
        self.l_base = "A:" + str(bases[0]) + str(bases[1])
        if not os.path.exists(self.l_base):
            os.mkdir(self.l_base)

    def convert(self):
        fname, ext = os.path.splitext(self.in_file_name)
        self.out_file_name = fname + self.ext
        self.out_file = os.path.join(self.l_base, self.out_file_name)


class PMover:
    CONVERT_EXTS = {'.dv': 'video/x-dv',
                    '.mxf': 'video/mxf'}

    def __init__(self, source_file: str=None) -> None:
        self._build_basic_logger()
        self.logger = logging.getLogger("AMover")
        self.source_file = open(os.path.join("L:\\Intranet\\ar\Digital_Services\\Inventory\\004_COMPLETED", source_file), 'r')
        self.success_move = open("L:\\Intranet\\ar\\Digital_Services\\Inventory\\005_A_COMPLETE\\{}"
                                 .format(source_file.split(".")[0] + ".tsv"), 'w')
        self.review = open("L:\\Intranet\\ar\\Digital_Services\\Inventory\\007_A_REVIEW\\{}".format("A_MOVES.tsv"),
                           'a')
        self.current_path_origin = str()
        self.num_files_in_path = int()

    def _build_basic_logger(self):
        log_dir = os.path.join(os.getcwd(), 'logs')
        self.logger_template_path = os.path.join(log_dir, 'logger_template.yml')
        f = open(self.logger_template_path, 'r')
        yml = yaml.safe_load(f)
        f.close()
        yml['handlers']['error_file_handler']['filename'] = os.path.join(log_dir, 'error.log')
        yml['handlers']['info_file_handler']['filename'] = os.path.join(log_dir, 'info.log')
        fh = open(os.path.join(log_dir, "logger_config.yml"), 'w')
        yaml.dump(yml, fh)
        fh.close()
        f = open(os.path.join(log_dir, "logger_config.yml"), 'r')
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    def next_path(self):
        for line in self.source_file.readlines():
            l = line.strip().split("\t")
            self.current_path_origin = l[0]
            yield l[1], os.path.join(l[1], "data")

    def get_next_file_path(self, p):
        for root, dirs, files in os.walk(p):
            for f in files:
                if f == "Thumbs.db":
                    continue
                yield root, f

    def magic_handling(self, cur_file: str):
        k = filetype.guess(cur_file)
        if k is None:
            return False
        mime = k.mime
        return mime

    def _get_numfiles_in_p(self, p):
        print("Analyizing path...")
        for __, __, files in os.walk(p):
            self.num_files_in_path += len(files)

    def _get_a_root(self, file_root):
        pre = file_root.split(os.path.sep)[1:]
        a_base = os.path.join("A:\\", "\\".join(pre))
        return a_base

    def _get_rel_file(self, cur_root, bag_path):
        r = os.path.relpath(cur_root, bag_path)
        if r == '.':
            return ''
        else:
            return r

    def _write_success(self, rc: RootConvert, f: MoveInfo):
        self.logger.info("COPIED: \t{} \t--->\t {}".format(rc.in_file, rc.out_file))
        self.success_move.write("{}\t{}".format(os.path.join(f.original_loc, f.file), rc.out_file))

    def _write_fail(self, rc: RootConvert, f: MoveInfo, error_msg: str):
        self.logger.info("NOT COPIED: \t{} \t--->\t {}".format(rc.in_file, error_msg))
        self.review.write("{}\t{}\t{}".format(os.path.join(f.original_loc, f.file), rc.in_file, rc.out_file))

    def _handle_img(self, f: MoveInfo):
        rc = RootConvert(f.p_file, ".jpg")
        ci = ConvertImgFile(f.p_file, f.mime, f.a_file)
        if os.path.exists(ci.img_out):
            self._write_success(rc, f)
            return

        if not ci.needs_conversion():
            copyfile(rc.in_file, rc.out_file)
        else:
            if ci.convert(rc.out_file):
                self._write_success(rc, f)
            else:
                self._write_fail(rc, f, ci.error_msg)

    def _handle_audio(self, f: MoveInfo):
        rc = RootConvert(f.p_file, ".m4a")
        ca = ConvertAudioFile(rc.in_file, rc.out_file)
        if os.path.exists(rc.out_file):
            self._write_success(rc, f)
            return

        if ca.needs_conversion(f.mime):
            if ca.convert(rc.out_file):
                self._write_success(rc, f)
            else:
                self._write_fail(rc, f, "Process did not complete")
        else:
            copyfile(rc.in_file, rc.out_file)

    def _handle_video(self, f: MoveInfo):
        rc = RootConvert(f.p_file, ".m4v")
        va = ConvertVideoFile(rc.in_file, rc.out_file)
        if os.path.exists(rc.out_file):
            self._write_success(rc, f)
            return

        if va.needs_conversion(f.mime):
            if va.convert(rc.out_file):
                self._write_success(rc, f)
            else:
                self._write_fail(rc, f, "Process did not complete")
        else:
            copyfile(rc.in_file, rc.out_file)

    def move(self):
        for base_path, bag_path in self.next_path():
            self.logger.info("MOVING: \t{}".format(base_path))
            for root, file in self.get_next_file_path(bag_path):

                p_file = os.path.join(root, file)
                a_root = self._get_a_root(base_path)
                a_file = os.path.join(a_root, file)
                original_loc = os.path.join(self.current_path_origin, self._get_rel_file(root, bag_path))
                mime = self.magic_handling(os.path.join(root, file))
                finfo = MoveInfo((p_file, a_root, None, original_loc, mime, file, base_path))

                if not mime:
                    copyfile(p_file, a_file)
                    self.logger.info("COPIED: \t{} \t--->\t {}".format(p_file, a_file))
                    self.success_move.write("{}\t{}".format(os.path.join(original_loc, file), a_file))
                    continue

                if mime.split("/")[0] == "image":
                    self._handle_img(finfo)
                    continue

                if mime.split("/")[0] == "audio":
                    self._handle_audio(finfo)
                    continue

                if mime.split("/")[0] == "video":
                    self._handle_video(finfo)
                    continue

    def close(self):
        self.source_file.close()
        self.success_move.close()
        self.review.close()

def file_chooser():
    base_path = "L:\\Intranet\\ar\Digital_Services\\Inventory\\004_COMPLETED"
    files = os.listdir(base_path)
    for i in range(len(files)):
        print("{})\t{}".format(i, files[i]))

    sel = input("Which file do you want to process: ")
    return files[int(sel)]


if __name__ == "__main__":
    sel = file_chooser()
    pm = PMover(sel)
    pm.move()
    pm.close()
    print()
