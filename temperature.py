import pandas as pd


test_file = "1901"


def all_temps():
    df = pd.read_csv(test_file)
    for index, row in df.iterrows():
        print(row[0].strip()[87:92])


def max_temps():
    df = pd.read_csv(test_file)
    temps = []
    for index, row in df.iterrows():
        infos = row[0].strip()
        temp = infos[87:92]
        if temp != "+9999":
            temps.append(int(temp))
    print(f"Max temp : {max(temps)}")


if __name__ == '__main__':
    all_temps()
    max_temps()
