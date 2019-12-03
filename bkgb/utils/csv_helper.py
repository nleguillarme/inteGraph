import csv
import sys
import pandas as pd


class ColumnNotFoundException(Exception):
    pass


def read_csv_header(csv_file, delimiter=","):
    csv.field_size_limit(sys.maxsize)
    with open(csv_file, "r") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader)
        f.close()
    # pd.read_csv(self.csv_file, sep=None, nrows=0).columns.tolist()
    return header


def get_csv_file_reader(csv_file, columns, delimiter=",", chunksize=100000):
    header = read_csv_header(csv_file, delimiter)
    for column in columns:
        if column not in header:
            raise ColumnNotFoundException()
    df_chunk = pd.read_csv(
        csv_file, sep=delimiter, chunksize=chunksize, usecols=columns
    )
    return df_chunk
