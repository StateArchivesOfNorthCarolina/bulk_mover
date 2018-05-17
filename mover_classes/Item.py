import os
import inspect
import shutil
import re
import pickle


class Incrementer:

    def __init__(self, seed: int=38) -> None:

        if not os.path.exists("inc.pkl"):
            self.incr = seed
            self.p_file = open('inc.pkl', 'wb')
            self.p_file.close()
        else:
            self.p_file = open('inc.pkl', 'rb')
            self.incr = pickle.load(self.p_file)
            self.p_file.close()

    def get_next_incr(self):
        self.incr += 1
        pickle.dump(self.incr, open('inc.pkl', 'wb'))
        return self.incr

    def set_inc(self, i):
        self.incr = i
        pickle.dump(self.incr, open('inc.pkl', 'wb'))


class ItemBase(object):
    def __init__(self, l: list=None) -> None:
        self.original_list = l
        self.p_location = None
        self.current_location = None
        self.record_status = None
        self.collection_type = None
        self.record_group = None
        self.series = None
        self.item = None
        self.accession_number = None
        self.bagged = None

    def assign_slots(self):
        pass

    def is_valid_path(self) -> bool:
        if os.path.isdir(self.current_location):
            return True
        else:
            return False

    def is_bagged(self) -> bool:
        return self.bagged

    def get_p_root(self)-> str:
        pass

    def get_a_root(self)-> str:
        pass

    def get_series_as_formatted_str(self):
        return self.series.replace(".", "_").zfill(5)


class PItem(ItemBase):
    def __init__(self, l: list, preprocessed: bool) -> None:
        super().__init__(l)
        self.pre = preprocessed
        if not self.pre:
            self.assign_slots()
            self.p_location = self.get_p_root()

    def assign_slots(self):
        l = self.original_list
        self.current_location = l[0]
        self.record_status = l[1]
        self.collection_type = l[2]
        self.record_group = l[3]
        if len(l) > 4 and l[4] != '':
            self.series = l[4]
        if len(l) > 5 and l[5] != '':
            self.item = l[5]
        if len(l) > 6 and l[6] != '':
            self.accession_number = l[6]
        self.bagged = None

    def get_p_root(self):
        if self.p_location is None:
            base = os.path.join("P:\\",
                                self.record_status,
                                self.collection_type,
                                self.record_group.zfill(5))

            if self._get_lvl4()[0]:
                base = os.path.join(base, self._get_lvl4()[1])

            if self._get_lvl5()[0]:
                base = os.path.join(base, self._get_lvl5()[1])

            if self._get_lvl6()[0]:
                base = os.path.join(base, self._get_lvl6()[1])
            self.p_location = base
            return base
        else:
            return self.p_location

    def _get_lvl3(self):
        if self.collection_type != "SR":
            return self.record_group
        else:
            return "{}".format(self.record_group.zfill(5))

    def _get_lvl4(self):
        if self.series is None:
            return False, ''
        return True, self.get_series_as_formatted_str()

    def _get_lvl5(self):
        if self.item is None:
            return False, ''
        return True, "{}".format(self.item.zfill(5))

    def _get_lvl6(self):
        if self.accession_number is None:
            return False, ''
        return True, str(self.accession_number)

    def do_both_exist(self):
        if os.path.exists(self.current_location) and os.path.exists(self.get_p_root()):
            return True
        return False


class AItem(ItemBase):
    def __init__(self, p: str) -> None:
        super().__init__([])
        self.current_location = p
        s = os.path.split(p)[1:]
        self.destination_location = s
        self.child_paths = []
        self.bag_root = os.path.join(self.current_location, "data")
        self.file_list = []
        self.files_in_path = 0

    def get_a_root(self):
        return self.destination_location

    @staticmethod
    def _get_path_as_a(p_path):
        p = os.path.split(p_path)[1:]
        return os.path.join("A:", p)

    def get_next_dir(self):
        for root, dirs, __ in os.walk(self.bag_root):
            for d in dirs:
                source = os.path.join(root, d)
                yield (source, self._get_path_as_a(source))

    @staticmethod
    def get_next_file(directory):
        for root, __, files in os.walk(directory):
            for f in files:
                yield (f, files)

    def get_paths(self):
        for root, dirs, files in os.walk(self.bag_root):
            for d in dirs:
                pass
            for f in files:
                self.child_paths.append(os.path.join(root, f))

    def is_confidential(self, p: str):
        reg = re.compile("CONFIDENTIAL")
        if reg.match(p):
            return True
        return False

    def write_confidential(self):
        print()


if __name__ == '__main__':
    inc = Incrementer()
    inc.set_inc(44)
    #print(inc.get_next_incr())
