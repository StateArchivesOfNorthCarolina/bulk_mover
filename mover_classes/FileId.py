import magic


class FileId(object):
    def __init__(self, file_to_id: str) -> None:
        self.file_to_id = file_to_id
        self.the_type = None    # type: str

    def _analyze(self):
        mime = magic.Magic(mime=True)
        t = mime.from_file(self.file_to_id).split("/")
        if len(t) != 2:
            self.the_type = "UNK"
        else:
            self.the_type = t[0]

    def is_video(self) -> bool:
        if self.the_type == "video":
            return True
        return False

    def is_image(self) -> bool:
        if self.the_type == "image":
            return True
        return False
