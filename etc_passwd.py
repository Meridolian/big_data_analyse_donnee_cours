import csv
import pandas as pd


test_file = "/etc/passwd"


def py_only():
    with open(test_file, "r") as file:
        lines = file.readlines()
        for line in lines:
            infos = line.split(":")
            print(f"name: {infos[0]}, uid: {infos[2]}, command: {infos[-1]}")


def csv_only():
    with open(test_file, "r") as file:
        lines = csv.reader(file, delimiter=":")
        for line in lines:
            print(f"name: {line[0]}, uid: {line[2]}, command: {line[-1]}")


def with_pandas():
    df = pd.read_csv(test_file, sep=":")
    df.columns = ["name", "x", "uid", "gid", "gecos", "home", "shell"]
    for index, row in df.iterrows():
        print(f"name: {row['name']}, uid: {row['uid']}, command: {row['shell']}")


if __name__ == '__main__':
    py_only()
    csv_only()
    with_pandas()
