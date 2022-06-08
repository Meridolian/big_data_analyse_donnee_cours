import pandas as pd
from pyarrow import csv


test_file = "1901"


########## PYTHON ONLY ##########


def python_all_temps():
    with open(test_file, "r") as file:
        lines = file.readlines()
        for line in lines:
            print(line.strip()[87:92])


def python_max_temps():
    temps = []
    with open(test_file, "r") as file:
        lines = file.readlines()
        for line in lines:
            temp = line.strip()[87:92]
            if temp != "+9999":
                temps.append(int(temp))
    print(f"max temp : {max(temps)}")


########## PANDAS ##########

def pandas_all_temps():
    df = pd.read_csv(test_file)
    for index, row in df.iterrows():
        print(row[0].strip()[87:92])


def pandas_max_temps():
    df = pd.read_csv(test_file)
    temps = []
    for index, row in df.iterrows():
        infos = row[0].strip()
        temp = infos[87:92]
        if temp != "+9999":
            temps.append(int(temp))
    print(f"Max temp : {max(temps)}")


########## PYARROW ##########


def pyarrow_all_temps():
    df = csv.read_csv(test_file)
    for line in df[0]:
        print(str(line)[87:92])


def pyarrow_max_temps():
    df = csv.read_csv(test_file)
    temps = []
    for line in df[0]:
        temp = str(line)[87:92]
        if temp != "+9999":
            temps.append(int(temp))
    print(f"max temp : {max(temps)}")


########## PYSPARK ##########


if __name__ == '__main__':
    pyarrow_max_temps()
