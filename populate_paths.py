import os
import sys
from mover_classes.PathUnique import PathUnique


def file_chooser():
    base_path = "L:\\Intranet\\ar\Digital_Services\\Inventory\\004_COMPLETED"
    files = os.listdir(base_path)
    for i in range(len(files)):
        i += 1
        print("{})\t{}".format(i, files[i-1]))

    sel = input("Which file do you want to process: ")
    return os.path.join(base_path, files[int(sel) - 1])


def add_to_paths():
    pu = PathUnique()
    with open(file_chooser(), 'r') as fh:
        for line in fh.readlines():
            s = line.strip().split("\t")
            if not pu.is_unique(s[1]):
                print(pu.current_path[0], pu.current_path[1])
                continue
            pu.add_to_paths(s[0], s[1])

    pu.save()


def examine_paths():
    pu = PathUnique()
    for k, v in pu.print_paths():
        print("{}\t{}".format(k, v[0]))


if __name__ == '__main__':
    args = sys.argv[1]
    if args == "a":
        add_to_paths()
        exit(0)

    if args == "p":
        examine_paths()
        exit(0)