import csv
import sys
import pandas as pd


class ColumnNotFoundException(Exception):
    pass


def read(file_path, sep="\t", index_col=None):
    df = pd.read_csv(file_path, sep=sep, dtype=str, encoding_errors="ignore", index_col=index_col)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    return df


def write(df, file_path, sep="\t", index=False):
    df.to_csv(file_path, sep=sep, index=index)


def read_csv_header(csv_file, delimiter=","):
    csv.field_size_limit(sys.maxsize)
    with open(csv_file, "r", errors="ignore") as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader)
        f.close()
    return header


def get_nb_records_in_csv(csv_file, delimiter=",", with_header=True):
    csv.field_size_limit(sys.maxsize)
    print(csv_file)
    with open(csv_file, newline="", errors="ignore") as f:
        reader = csv.reader(f, delimiter=delimiter)
        rec_count = sum(1 for row in reader)
        f.close()
    return rec_count - 1 if with_header else rec_count


def get_csv_file_reader(
    csv_file, columns=None, dtype={}, delimiter=",", chunksize=100000
):
    header = read_csv_header(csv_file, delimiter)
    if columns:
        for column in columns:
            if column not in header:
                raise ColumnNotFoundException()
    df_reader = pd.read_csv(
        csv_file,
        sep=delimiter,
        header=0,
        skipinitialspace=True,
        chunksize=chunksize,
        usecols=columns,
        dtype=dtype,
        encoding_errors="ignore",
    )
    return df_reader
