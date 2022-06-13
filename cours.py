import pandas as pd
from pyarrow import csv, compute
import pyarrow as pa


temps_1901 = "1901"
temps_1999 = "1999"
loc_file = "master-location-identifier-database-20130801.csv"


########## PYTHON ONLY ##########


def python_all_temps():
    with open(temps_1901, "r") as file:
        lines = file.readlines()
        for line in lines:
            print(line.strip()[87:92])


def python_max_temps():
    temps = []
    with open(temps_1901, "r") as file:
        lines = file.readlines()
        for line in lines:
            temp = line.strip()[87:92]
            if temp != "+9999":
                temps.append(int(temp))
    print(f"max temp : {max(temps)}")


########## PANDAS ##########

def pandas_all_temps():
    df = pd.read_csv(temps_1901)
    for index, row in df.iterrows():
        print(row[0].strip()[87:92])


def pandas_max_temps():
    df = pd.read_csv(temps_1901)
    temps = []
    for index, row in df.iterrows():
        infos = row[0].strip()
        temp = infos[87:92]
        if temp != "+9999":
            temps.append(int(temp))
    print(f"Max temp : {max(temps)}")


########## PYARROW ##########


def pyarrow_max_temps():
    table = csv.read_csv(temps_1901, read_options=csv.ReadOptions(
        column_names=["data"]), parse_options=csv.ParseOptions(delimiter="|")
    )
    tableFormat = compute.utf8_slice_codeunits(table.column("data"), 87, 92)
    tableFormat = compute.replace_substring(tableFormat, "+9999", "")
    tableFormat = compute.replace_substring(tableFormat, "-", " ")
    print(compute.max(tableFormat))


def pyarrow_mean_temps():
    table = csv.read_csv(temps_1901, read_options=csv.ReadOptions(
        column_names=["Temperatures"]
    ))
    temp = compute.utf8_slice_codeunits(table["Temperatures"], 87, 92)
    temp = compute.replace_substring(temp, "+9999", "0")
    temp = compute.replace_substring(temp, "+", "0")
    temp = compute.cast(temp, pa.int64())
    temp_mean = compute.mean(temp)
    print(temp_mean)


def pyarrow_mean_per_year():
    table = csv.read_csv(temps_1999, read_options=csv.ReadOptions(column_names=["Temperatures"]))
    col_temp = compute.utf8_slice_codeunits(table["Temperatures"], 87, 92)
    col_temp = compute.replace_substring(col_temp, "+", "0")
    col_temp = compute.cast(col_temp, pa.int64())
    col_year = compute.utf8_slice_codeunits(table["Temperatures"], 15, 19)
    new_table = pa.Table.from_arrays([col_year, col_temp], names=["year", "temp"])
    filtered_table = new_table.filter(compute.not_equal(new_table["temp"], 9999))
    mean_per_year = pa.TableGroupBy(filtered_table, "year").aggregate([
        ("temp", "mean"), ("temp", "max"), ("temp", "min"), ("temp", "stddev")
    ])
    print(mean_per_year)


def pyarrow_max_temp_localisation_per_year():
    loc_table = csv.read_csv(loc_file)
    temp_table = csv.read_csv(temps_1999, read_options=csv.ReadOptions(column_names=["Temperatures"]))
    col_wban = compute.utf8_slice_codeunits(temp_table["Temperatures"], 4, 10)
    col_wban = compute.cast(col_wban, pa.int64())
    col_temp = compute.utf8_slice_codeunits(temp_table["Temperatures"], 87, 92)
    col_temp = compute.replace_substring(col_temp, "+", "0")
    col_temp = compute.cast(col_temp, pa.int64())
    col_year = compute.utf8_slice_codeunits(temp_table["Temperatures"], 15, 19)
    new_table = pa.Table.from_arrays([col_year, col_temp, col_wban], names=["year", "temp", "maslib"])
    filtered_table = new_table.filter(compute.not_equal(new_table["temp"], 9999))
    filtered_table = pa.TableGroupBy(filtered_table, "year").aggregate([("temp", "max")])
    joined_table = loc_table.join(filtered_table, loc_table["maslib"], filtered_table["maslib"])
    print(loc_table)


########## PYSPARK ##########


if __name__ == '__main__':
    pyarrow_max_temp_localisation_per_year()
