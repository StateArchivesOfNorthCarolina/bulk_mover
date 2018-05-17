import subprocess
import os


class ConvertFile:
    def __init__(self, fin: str, fout: str=None) -> None:
        self.fin = fin
        self.fout = fout
        self.error = str()
        self.success = str()
        self.mime = None  # type: str
        self.no_access_mimes = {"application/zip": ".txt",
                                "application/x-tar": ".txt",
                                "application/x-rar-compressed": ".txt",
                                "application/gzip": ".txt",
                                "application/x-bzip2": ".txt",
                                "application/x-7z-compressed": ".txt",
                                "application/x-xz": ".txt",
                                "application/x-msdownload": ".txt",
                                "application/octet-stream": ".txt",
                                "application/x-sqlite3": ".txt",
                                "application/x-nintendo-nes-rom": ".txt",
                                "application/x-google-chrome-extension": ".txt",
                                "application/vnd.ms-cab-compressed": ".txt",
                                "application/x-deb": ".txt",
                                "application/x-unix-archive": ".txt",
                                "application/x-compress": ".txt",
                                "application/x-lzip": ".txt",
                                "application/font-woff": ".txt",
                                "application/font-sfnt": ".txt",
                                "sanc_no_access_plain": ".txt"
                                }

    def get_filename_parts(self):
        fname = os.path.splitext(os.path.basename(self.fin))
        return fname[0], fname[1]

    def get_out_dir(self):
        return os.path.dirname(self.fout)

    def convert(self):
        pass

    def is_no_access_file(self):
        if self.mime is None:
            return False
        if self.mime.lower() in self.no_access_mimes.keys():
            return True
        return False


class ConvertDocumentFile(ConvertFile):
    def __init__(self, fin: str, fout: str=None) -> None:
        super().__init__(fin, fout)
        self.out_ext = ".pdf"
        self.base_opts = ['C:\\Program Files\\LibreOffice\\program\\soffice.exe', '--headless',
                          '--convert-to', 'pdf:writer_pdf_Export', '--outdir']

    def convert(self):
        self.base_opts.append(self.get_out_dir())
        self.base_opts.append(self.fin)
        try:
            proc = subprocess.run(self.base_opts, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            if proc.returncode == 0:
                return True
        except subprocess.TimeoutExpired as e:
            self.error = "Conversion did not complete within 30 seconds."
            return False
        return False


class ConvertNoAccessFile(ConvertFile):

    def __init__(self, fin: str, fout: str=None) -> None:
        super().__init__(fin, fout)
        self.explain_text = '''
                            #### AS OF REPOSITORY MOVE 2017-2018 THIS FILE HAS NO ACCESS COPY  ####
                            #### Contact the Digital Archivist in the Digital Services Section ####
                            #### to request access.                                            ####
                            '''

    def convert(self):
        try:
            with open(self.fout, 'w') as fh:
                fh.write(self.explain_text)
            fh.close()
            return True
        except IOError or FileExistsError as e:
            self.error = e
            return False