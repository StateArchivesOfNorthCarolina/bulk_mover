import os


class CheckReview:
    def __init__(self, file: str) -> None:
        self.file = file
        self.file_no_ext = os.path.split(file)[1].split(".")[0]
        self.report = os.path.join(os.path.split(self.file)[0], self.file_no_ext + "_reviewed.tsv")

    def check(self):
        with open(self.report, "a") as wh:
            with open(self.file, 'r') as fh:
                for line in fh.readlines():
                    sp = line.strip().split("\t")
                    check = sp[1]
                    if check is 'None':
                        wh.write("{} \t Not copied\n".format(sp[0]))
                        continue

                    if os.path.exists(check):
                        wh.write("{} \t Copied\n".format(sp[0]))
                    else:
                        wh.write("{} \t Not copied\n".format(sp[0]))


def file_chooser():
    base_path = "L:\\Intranet\\ar\\Digital_Services\\Inventory\\007_A_REVIEW"
    files = os.listdir(base_path)
    for i in range(len(files)):
        print("{})\t{}".format(i, files[i]))

    sel = input("Which file do you want to process: ")
    return os.path.join(base_path, files[int(sel)])


if __name__ == '__main__':
    fi = file_chooser()
    cr = CheckReview(fi)
    cr.check()