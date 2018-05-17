from pathlib import Path

if __name__ == '__main__':
    test = "S:\Staging"
    p = Path(test)
    for pt in p.iterdir():
        print(pt)