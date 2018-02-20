from shutil import copyfileobj
import shutil
from tqdm import tqdm
import os
import sys


class CopyProgress:
    FILES = 0
    BYTES = 1

    def __init__(self, p_from: str=None, p_to: str=None, files_or_bytes: int=FILES) -> None:
        self.p_orig = p_from
        self.p_dest = p_to
        self.total_files = 0
        self.total_bytes = 0
        self.file_pack = []
        self.counter_type = files_or_bytes
        self.pbar = None  #type: tqdm
        self.copyobj = self._copyfileobj_patched

    def _copyfileobj_patched(self, fsrc, fdst, length=32*1024*1024):
        fn = os.path.dirname(fsrc.name)
        self.pbar.set_description(fn, refresh=False)
        while 1:
            buf = fsrc.read(length)
            if self.counter_type == self.BYTES:
                self.pbar.update(length)
            if not buf:
                break
            fdst.write(buf)
        if self.counter_type == self.FILES:
            self.pbar.update(1)

    def _init_pbar_files(self, descrip=None,):
        self.pbar = tqdm(total=self.total_files, ascii=True, desc=descrip)

    def _init_pbar_bytes(self, descrip=None):
        self.pbar = tqdm(total=self.total_bytes, unit='B', ascii=True, unit_scale=True, unit_divisor=1024, desc=descrip)

    def _init_counter(self):
        tqdm.write("Scanning: \t {}".format(self.p_orig))
        for root, __, files in tqdm(os.walk(self.p_orig)):
            self.total_files += len(files)
            if self.counter_type == self.BYTES:
                for f in files:
                    self.total_bytes += os.path.getsize(os.path.join(root, f))

    def start_copy(self):
        self._init_counter()
        shutil.copyfileobj = self.copyobj
        try:
            if self.counter_type == self.FILES:
                self._init_pbar_files()
                shutil.copytree(self.p_orig, self.p_dest)
            elif self.counter_type == self.BYTES:
                self._init_pbar_bytes()
                shutil.copytree(self.p_orig, self.p_dest)
        except shutil.Error as e:
            self.pbar.close()
            return False
        self.pbar.close()
        return True


if __name__ == "__main__":
    cp = CopyProgress(p_from='H:\\GitHub', p_to='H:\\New_Github',
                      files_or_bytes=CopyProgress.FILES)
    if os.path.exists("H:\\New_Github"):
        shutil.rmtree("H:\\New_Github")
    cp.start_copy()