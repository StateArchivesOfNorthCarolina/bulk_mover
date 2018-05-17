import os
from shutil import copyfile
import yaml
import logging
import logging.config

from datetime import date
from datetime import datetime
from peewee import *
from mover_classes.PathMunger import PathMunger
from mover_classes.ConvertImgFile import ConvertImgError
import re

db = SqliteDatabase('ptoa_conversion.db', pragmas=(('foreign_keys', 'on'),))


class BaseModel(Model):
    class Meta:
        database = db


class PtoADB(BaseModel):
    p_root = TextField(unique=True)
    completed_conversion = BooleanField(default=False)
    date_completed = DateTimeField(null=True)


class PtoAFiles(BaseModel):
    root = ForeignKeyField(PtoADB)
    p_file_name = TextField()
    p_file_size = BigIntegerField()
    a_file_name = TextField()
    a_file_size = BigIntegerField()
    completed = BooleanField(default=False)
    date_completed = DateTimeField()

db.connect()

try:
    db.create_tables([PtoADB, PtoAFiles])
except OperationalError as e:
    pass


class PMover:

    def __init__(self, source_file: str=None, drivel: str=None) -> None:
        self._build_basic_logger()
        self.logger = logging.getLogger("AMover")
        self.source_file = open(os.path.join("L:\\Intranet\\ar\Digital_Services\\Inventory\\004_COMPLETED", source_file), 'r')
        s = source_file.split(".")[0]
        self.success_move = open("L:\\Intranet\\ar\\Digital_Services\\Inventory\\005_A_COMPLETE\\{}.tsv"
                                 .format(s), 'w')

        self.review = open("L:\\Intranet\\ar\\Digital_Services\\Inventory\\007_A_REVIEW\\{}.tsv".format(s),
                           'w')
        self.current_path_origin = str()
        self.num_files_in_path = int()
        self.dest_drive = "T:"
        self.restricted_dir = False
        if drivel is not None:
            self.dest_drive = drivel

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
            yield l[1]

    @staticmethod
    def get_next_file_path(p):
        for root, dirs, files in os.walk(p):
            for f in files:
                if f == "Thumbs.db":
                    continue
                yield root, f

    def _get_numfiles_in_p(self, p):
        print("Analyizing path...")
        for __, __, files in os.walk(p):
            self.num_files_in_path += len(files)

    def _handle_img(self, pm: PathMunger):
        try:
            cif = pm.get_img_converter()
            pm.set_dest_file_name(cif.which_ext())
            cif.img_out = pm.get_dest_file_path()
            if pm.is_dest_there():
                return True

            if not cif.needs_conversion():
                if not pm.do_a_copy():
                    return False
                return True

            if cif.convert():
                return True
            return False
        except ConvertImgError:
            return False

    def _handle_audio(self, pm: PathMunger):
        caf = pm.get_audio_converter()
        caf.mime = pm.source_mime
        pm.set_dest_file_name(caf.which_ext())
        caf.av_out = pm.get_dest_file_path()
        if pm.is_dest_there():
            return True

        if not caf.needs_conversion():
            if not pm.do_a_copy():
                return False
            return True

        if pm.is_dest_there():
            # Write Success
            return True
        if not caf.convert():
            # Fail
            return False

    def _handle_video(self, pm: PathMunger):
        cav = pm.get_video_converter()
        cav.mime = pm.source_mime
        pm.set_dest_file_name(cav.which_ext())
        cav.av_out = pm.dest_file_path
        if pm.is_dest_there():
            return True

        if not cav.needs_conversion():
            pm.do_a_copy()
            return True

        if cav.convert():
            return True
        return False

    def _handle_document(self, pm: PathMunger):
        cdf = pm.get_document_converter()
        pm.set_dest_file_name(".pdf")
        cdf.fout = pm.get_dest_file_path()
        cdf.mime = pm.source_mime
        # Is this a no_access_file
        if cdf.is_no_access_file():
            if self._handle_no_access(pm):
                return True
            return False

        # No proceed with potential conversion
        if pm.is_dest_there():
            return True
        elif not pm.needs_conversion():
            pm.do_a_copy()
            return True

        if cdf.convert():
            return True
        return False

    def _handle_no_access(self, pm: PathMunger):
        cna = pm.get_noaccess_converter()
        pm.set_dest_file_name(".txt")
        cna.fout = pm.get_dest_file_path()
        if pm.is_dest_there():
            return True
        if cna.convert():
            return True
        return False

    def _write_success(self, pm: PathMunger):
        self.logger.info(pm.get_success_message()[0])
        self.success_move.write(pm.get_success_message()[1])
        self.__write_ptoa(pm)

    def _write_fail(self, pm: PathMunger):
        self.logger.info(pm.get_fail_message(pm.get_error())[0])
        self.review.write(pm.get_fail_message(pm.get_error())[1])
        self.__write_ptoa(pm, False)

    def _handle_conversion(self, pm: PathMunger):
        mime = pm.get_mime_type()

        if mime == "image":
            if self._handle_img(pm):
                return True
        if mime == "audio":
            if self._handle_audio(pm):
                return True
        if mime == "video":
            self._handle_video(pm)
        if mime == "sanc_document" or mime == "application":
            if self._handle_document(pm):
                return True
        if mime == "sanc_no_access":
            if self._handle_no_access(pm):
                return True

        return False

    def _handle_restricted(self, pm: PathMunger):
        pm.is_no_access = True
        if self._handle_no_access(pm):
            self._write_success(pm)
            return True
        else:
            self._write_fail(pm)
            return False

    def _is_restricted(self, pm: PathMunger):
        rest_strings = [r'\NP\SR', r'\RS']
        for i in rest_strings:
            if pm.source_file.__contains__(i):
                return True
        return False

    def _is_a_blank_path(self, p: str):
        regexs = [r"^.*(\\\\RS)",
                  r"^.*(\\\\GS)",
                  r"^.*(\\06002)",
                  r"^.*(\\33435)",
                  r"Email",
                  r"email",
                  r"database",
                  r"Database"]

        if re.match("|".join('(?:{0})'.format(x) for x in regexs), p):
            return True
        return False

    def quick_move(self):
        pass

    def __write_ptoa(self, pm: PathMunger, success=True):
        ptoa_files = PtoAFiles()
        ptoa_files.root = globals()['__P2ADB__']
        ptoa_files.p_file_name = pm.get_source_file_path()
        ptoa_files.p_file_size = os.path.getsize(pm.get_source_file_path())
        ptoa_files.a_file_name = pm.get_dest_file_path()
        try:
            ptoa_files.a_file_size = os.path.getsize(pm.get_dest_file_path())
        except OSError as e:
            ptoa_files.a_file_size = 0

        if success:
            ptoa_files.completed = True
            ptoa_files.date_completed = datetime.now()
        else:
            ptoa_files.completed = False
            ptoa_files.date_completed = datetime.now()
        ptoa_files.save()

    def move(self):
        for base_path in self.next_path():
            pm = PathMunger(base_path, "T:")
            self.logger.info("CONVERTING: \t{}".format(pm.get_source_bag()))
            p_root_db = None
            l = []
            try:
                p_root_db = PtoADB.create(p_root=pm.get_source_bag())
            except Exception as e:
                p_root_db = PtoADB.get(PtoADB.p_root == pm.get_source_bag())
                if p_root_db.completed_conversion:
                    self.logger.info("Already Converted: \t{}".format(pm.get_source_bag()))
                    continue
                else:
                    # Find files in this root that are converted
                    for f in PtoAFiles.select().where(PtoAFiles.root == p_root_db):
                        l.append(f.p_file_name)

            globals()['__P2ADB__'] = p_root_db
            for root, file in self.get_next_file_path(pm.get_source_bag()):
                if os.path.join(root, file) in l:
                    continue
                pm = PathMunger(base_path, "T:")
                pm.set_current_targets(root, file)
                pm.create_dest_path()
                # Handle Restricted Files
                if self._is_a_blank_path(pm.source_base):
                    if self._handle_restricted(pm):
                        self._write_success(pm)
                    else:
                        self._write_fail(pm)
                    continue

                ext = os.path.splitext(file)[1]
                if len(ext) < 4 or len(ext) > 5:
                    # Extension will not be determinable go ahead and copy
                    if pm.do_a_copy():
                        self._write_success(pm)

                if pm.is_pass_through():
                    if pm.is_dest_there():
                        self._write_success(pm)
                        continue
                    if pm.do_a_copy():
                        self._write_success(pm)
                        continue
                    self._write_fail(pm)
                else:
                    if self._handle_conversion(pm):
                        self._write_success(pm)
                    else:
                        self._write_fail(pm)

            p_root_db.completed_conversion = True
            p_root_db.date_completed = datetime.now()
            p_root_db.save()

    def close(self):
        self.source_file.close()
        self.success_move.close()
        self.review.close()


def file_chooser():
    base_path = "L:\\Intranet\\ar\Digital_Services\\Inventory\\004_COMPLETED"
    files = os.listdir(base_path)
    for i in range(len(files)):
        i += 1
        print("{})\t{}".format(i, files[i - 1]))

    sel = input("Which file do you want to process: ")
    return files[int(sel) - 1]


if __name__ == "__main__":
    sel = file_chooser()
    pm = PMover(sel)
    pm.move()
    pm.close()
    print()
