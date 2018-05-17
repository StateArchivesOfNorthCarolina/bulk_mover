import hashlib
import pickle
import os


class PathUnique:
    def __init__(self) -> None:
        self.paths = {}   #type: dict
        self.pickle_name = os.path.join(os.getcwd(), "unique_paths.pkl")
        self.current_key = str()
        self.current_path = str()
        self.__open()

    def __open(self):
        if os.path.exists(self.pickle_name):
            with open(self.pickle_name, 'rb') as fh:
                self.paths = pickle.load(fh)

    def is_unique(self, p: str):
        k = self.__get_hash(p)
        if k in self.paths.keys():
            self.current_key = k
            self.current_path = self.paths[k]
            return False
        return True

    def __get_hash(self, p: str):
        b2s = hashlib.blake2b(digest_size=10)
        b2s.update(bytearray(p, encoding="utf8"))
        return b2s.hexdigest()

    def add_to_paths(self, current: str, dest: str):
        self.paths[self.__get_hash(dest)] = (current, dest)

    def print_paths(self):
        for k, v in self.paths.items():
            yield k, v

    def save(self):
        with open(self.pickle_name, 'wb') as fh:
            pickle.dump(self.paths, fh)