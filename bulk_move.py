import os
import sys
import stat
from shutil import rmtree
import yaml
import logging
import logging.config
from bagit import BagValidationError
import time

from mover_classes.Item import PItem
from Scripts.sanc_bagger import SANCBagger
from mover_classes.CopyProgress import CopyProgress
from mover_classes.PathUnique import PathUnique
from mover_classes.MoverDBs import *


class Mover:
    NEW_LOOP = 5
    KEEP_GOING = 4

    def __init__(self, move_items: [PItem], file_name=None) -> None:
        super().__init__()
        self._build_basic_logger()
        self.logger = logging.getLogger("Mover")
        self.file_name = file_name
        self.move_items = move_items
        self.moved_log = None
        self.not_moved_log = None
        self.last_validation_error = None
        self.moved = None
        self.review = None
        self.sanc_bagger = SANCBagger()

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

    def _remove_thumbs_in_bag(self, p: str):
        for root, __, files in os.walk(p):
            if "Thumbs.db" in files:
                self.logger.info("REMOVE:\t{}".format(os.path.join(root, "Thumbs.db")))
                os.remove(os.path.join(root, "Thumbs.db"))

    def _verify_destination(self, p_item: PItem):
        # This function assumes the source still exists and is valid.
        # Returns True if path is in place and complete
        # Returns False is path is in anyway not complete

        # 1) does destination exist
        if not os.path.exists(p_item.get_p_root()):
            return False

        # 2) Path exists
        #    Do both source and destination have the same number of files

        if not self._do_paths_have_same_num_files(p_item.current_location, p_item.get_p_root()):
            # File mismatch
            return False

        # 3) Number of files are the same
        #    is the Destination valid
        if not self._full_validate(p_item.get_p_root()):
            # Destination is not valid.
            return False
        return True

    def _fast_validate(self, p: str):
        self.sanc_bagger.bag_to_open = p
        if not self.sanc_bagger.open_bag():
            self.last_validation_error = self.sanc_bagger.which_error()
            return False
        if not self.sanc_bagger.fast_validate_bag():
            self.last_validation_error = self.sanc_bagger.which_error()
            return False
        else:
            return True

    def _full_validate(self, p: str):
        self.sanc_bagger.bag_to_open = p
        if not self.sanc_bagger.open_bag():
            self.last_validation_error = self.sanc_bagger.which_error()
            return False
        if not self.sanc_bagger.validate_bag():
            self.last_validation_error = self.sanc_bagger.which_error()
            return False
        else:
            return True

    def _do_paths_have_same_num_files(self, p1: str, p2: str):
        if self._count_files(p1) != self._count_files(p2):
            return False
        return True

    def _count_files(self, p: str):
        count = 0
        for __, __, files in os.walk(p):
            count += len(files)
        return count

    def _sanc_rmtree(self, tree):
        for root, dirs, __ in os.walk(tree, topdown=False):
            for d in dirs:
                print("\tREMOVING:\t{}".format(os.path.join(os.path.join(root, d))))
                rmtree(os.path.join(root, d), onerror=self._handle_remove_errors)
        rmtree(tree, onerror=self._handle_remove_errors)

    def _handle_remove_errors(self, f, p, e):
        os.chmod(p, stat.S_IWRITE)
        f(p)

    def _validate_source(self, p_item: PItem):
        # 1) Does the source still exists
        if not os.path.exists(p_item.current_location):
            self.logger.info("MOVED:\t{}\t{}".format(p_item.current_location, p_item.get_p_root()))
            self.moved.write("{}\t{}\t{}\t{}\n".format(p_item.current_location, p_item.get_p_root(),
                                                       time.strftime("%Y-%m-%d", time.gmtime()), "Script"))
            self.moved.flush()
            # No Move on to the next level_5.
            return Mover.NEW_LOOP

        # 2) Is the source valid
        self.logger.info("VALIDATING:\t{}".format(p_item.current_location))
        if not self._full_validate(p_item.current_location):
            # 2a) No. Is it because of Thumbs
            self.logger.info("NOT VALID:\t{}".format(p_item.current_location))
            self.logger.info("REMOVE THUMBS:\t{}".format(p_item.current_location))
            self._remove_thumbs_in_bag(p_item.current_location)
            self.logger.info("VALIDATING:\t{}".format(p_item.current_location))
            if not self._full_validate(p_item.current_location):
                # 2b) No. Punt
                self.logger.info("NOT VALID:\t{}".format(p_item.current_location))
                self.logger.info("NOT MOVED:\t{}".format(p_item.current_location))
                self.review.write("{}\t{}\t{}\t{}\n".format('NOT MOVED', p_item.current_location,
                                                            self.sanc_bagger.which_error()[0],
                                                            self.sanc_bagger.which_error()[1]))
                self.review.flush()
                # Done as much as we can with this one move on. Source is left in place
                return Mover.NEW_LOOP
        return Mover.KEEP_GOING

    def _verify_dest(self, p_item: PItem):
        if not os.path.exists(p_item.get_p_root()):
            self.logger.info("CLEAR:\t{}".format(p_item.get_p_root()))
            return Mover.NEW_LOOP

    def _remove_source(self, source):
        self.logger.info("REMOVING: {}".format(source))
        self._sanc_rmtree(source)
        self.logger.info("REMOVED: {}".format(source))

    def _remove_destination(self, dest):
        self.logger.info("REMOVING: {}".format(dest))
        self._sanc_rmtree(dest)
        self.logger.info("REMOVED: {}".format(dest))

    def start_move(self):
        self.logger.info("START\t{}".format(self.file_name))
        self.review = open(self.not_moved_log, 'w')
        self.moved = open(self.moved_log, 'w')
        pu = PathUnique()
        for p_item in self.move_items:

            if not pu.is_unique(p_item.get_p_root()):
                self.logger.error("DEST_NOT_UNIQUE: {}\n".format(p_item.get_p_root()))
                self.review.write("{}\t{}\t{}\t{}\t{}\n".format("DEST NOT UNIQUE",
                                                        p_item.current_location,
                                                        p_item.get_p_root(),
                                                        pu.current_path[0],
                                                        pu.current_path[1]))
                continue
            else:
                pu.add_to_paths(p_item.current_location, p_item.get_p_root())

            self.logger.info("BEGIN: \t{}\t{}".format(p_item.current_location, p_item.get_p_root()))
            # Verify the source #
            if p_item.do_both_exist():
                if self._full_validate(p_item.get_p_root()):
                    try:
                        self._remove_source(p_item.current_location)
                    except PermissionError as e:
                        self.review.write("{}\t{}\t{}\t{}\n".format("SOURCE NOT REMOVED", p_item.current_location, p_item.get_p_root(),
                                                                    e))
                        self.logger.error("SOURCE NOT REMOVED: \t{}".format(p_item.current_location))
                        continue
                    else:
                        self.moved.write("{}\t{}\t{}\t{}\n".format(p_item.current_location, p_item.get_p_root(),
                                                                   time.strftime("%Y-%m-%d", time.gmtime()), "Script"))
                        self.moved.flush()
                        self.logger.info("END: \t{}".format(p_item.current_location))
                        continue
                else:
                    self.logger.error("INVALID DEST: \t{}\t{}".format(p_item.get_p_root(), self.last_validation_error))

            if self._validate_source(p_item) == Mover.NEW_LOOP:
                self.logger.info("END: \t{}".format(p_item.current_location))
                continue

            self.logger.info("VALID:\t{}".format(p_item.current_location))

            # 3) Source Exists and is Valid. Let's move this package

            self.logger.info("VERIFYING:\t{}".format(p_item.get_p_root()))

            # Is the path clear at the destination?

            if self._verify_dest(p_item) == Mover.NEW_LOOP:
                # Path is clear. No failed attempts in the way.

                # # # # BIG EXECUTION POINT # # # #
                self._move_path(p_item)
                self.logger.info("END: \t{}".format(p_item.current_location))
                continue

            # Path was not clear. Is the destination valid
            if self._full_validate(p_item.get_p_root()):
                # Yes it is valid.
                # Remove the source
                # # # # # BIG EXECUTION POINT # # # #.
                self.logger.info("VALID:\t{}".format(p_item.get_p_root()))
                try:
                    self._remove_source(p_item.current_location)
                except PermissionError as e:
                    self.review.write("{}\t{}\t{}\t{}\n".format("SOURCE NOT REMOVED", p_item.current_location, p_item.get_p_root(),
                                                                e))
                    self.logger.error("SOURCE NOT REMOVED: \t{}".format(p_item.current_location))
                    continue
                else:
                    self.logger.info("END: \t{}".format(p_item.current_location))
                    continue

            # The destination package is invalid
            # remove destination and try moving the path again.
            self.logger.info("NOT VERIFIED: \t{}".format(p_item.get_p_root()))
            # # # # # BIG EXECUTION POINT # # # #.
            try:
                self._remove_destination(p_item.get_p_root())
            except PermissionError as e:
                self.review.write("{}\t{}\t{}\t{}\n".format("DEST NOT CLEAR", p_item.current_location, p_item.get_p_root(),
                                                        e))
                self.logger.error("DEST NOT CLEAR: \t{}".format(p_item.current_location))
                continue

            if self._move_path(p_item):
                self.logger.info("END: \t{}".format(p_item.current_location))
        self.review.close()
        self.moved.close()

    def _move_path(self, p_item: PItem) -> bool:
        self.logger.info("Source is valid and destination is clear.")
        self.logger.info("MOVING:\t{}:\t{}".format(p_item.current_location, p_item.get_p_root()))
        try:
            cp = CopyProgress("\\\\?\\" + p_item.current_location, p_item.get_p_root(), CopyProgress.FILES)
            cp.start_copy()
            self.logger.info("VALIDATING: {}".format(p_item.get_p_root()))
            if self._full_validate(p_item.get_p_root()):
                self.logger.info("VALID: {}".format(p_item.get_p_root()))
                self.logger.info("REMOVING: {}".format(p_item.current_location))
                self._sanc_rmtree(p_item.current_location)
                self.logger.info("REMOVED: {}".format(p_item.current_location))
                self.moved.write("{}\t{}\t{}\t{}\n".format(p_item.current_location, p_item.get_p_root(),
                                                   time.strftime("%Y-%m-%d", time.gmtime()), "Script"))
                self.moved.flush()
                return True
            else:
                self.logger.info("NOT VALID: {}".format(p_item.get_p_root()))
                self.review.write("{}\t{}\t{}\t{}\n".format(p_item.current_location, p_item.get_p_root(),
                                                            self.sanc_bagger.which_error()[0],
                                                            self.sanc_bagger.which_error()[1]))
                self.review.flush()
                return False
        except FileNotFoundError as e:
            self.logger.error("NOT FOUND: {}".format(p_item.current_location))
            self.review.write("{}\t{}\n".format(p_item.current_location, "NOT FOUND"))
            self.review.flush()
            return False
        except BagValidationError as e:
            self.logger.error("{}: {}".format(e, p_item.current_location))
            self.review.write("{}\t{}\n".format(p_item.current_location, e))
            self.review.flush()
            return False
        except Exception as e:
            self.logger.error("{}: {}".format(e, p_item.current_location))
            self.review.write("{}\t{}\n".format(p_item.current_location, e))
            self.review.flush()
            return False


def file_chooser():
    files = os.listdir("L:\\Intranet\\ar\Digital_Services\\Inventory\\002_TO_BE_MOVED")
    for i in range(len(files)):
        print("{})\t{}".format(i, files[i]))

    sel = input("Which file do you want to process: ")
    return files[int(sel)]


if __name__ == "__main__":
    args = sys.argv
    val = False
    pu = PathUnique()
    source_fn = file_chooser()
    pre = False
    if len(args) > 1:
        pre = True
    comp_move = open("L:\\Intranet\\ar\Digital_Services\\Inventory\\002_TO_BE_MOVED\\{}".format(source_fn), "r")
    moved_file_name = source_fn.split(".")[0]
    moved = "L:\\Intranet\\ar\\Digital_Services\\Inventory\\004_COMPLETED\\{}_MOVED.tsv".format(moved_file_name)
    not_moved = "L:\\Intranet\\ar\\Digital_Services\\Inventory\\003_NEEDS_REVIEW\\{}_REVIEW.tsv".format(moved_file_name)
    lines = []
    item_list = []

    for f in comp_move.readlines():
        l = f.strip().split("\t")
        if not pre:
            pi = PItem(l, False)
            pr = pi.get_p_root()
            if val:
                if pu.is_unique(pr):
                    item_list.append(pi)
                else:
                    print("NOT UNIQUE: {}\t{}".format(pi.current_location, pr))
                    sys.exit(-1)
            else:
                item_list.append(pi)
        else:
            p = PItem(l, True)
            p.p_location = l[1]
            p.current_location = l[0]
            item_list.append(p)
    if val:
        pu.save()
    mv = Mover(item_list, comp_move.name)
    mv.moved_log = moved
    mv.not_moved_log = not_moved
    mv.start_move()



