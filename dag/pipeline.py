import json
import schema
import numpy as np
import pandas as pd
import pandas_schema
import os
import time
import argparse

from helper.utils import check_decimal, check_int, check_date, check_timeformat
from pandas_schema.validation import CustomElementValidation
from pandas_schema import Column
from dask import dataframe as dd
from database.base_operation import DBConn, DBOperations
from datetime import datetime
from datetime import date, timedelta

conn = DBConn()
db_opr = DBOperations(conn)


class Pipeline:
    def __init__(self, date_=None):
        self.data = None
        self.date_filter = None

    def read_all_data(self, finput) -> dd:
        """
        Read csv file and return data based on date_filter
        """
        start = time.time()
        self.data = dd.read_csv(finput)
        end = time.time()
        print("Extract csv with dask:", round((end - start), 5), "sec")

    def read_data_based_key(
        self, finput=None, key_filter=None, date_filter=None
    ) -> dd:
        """
        Read csv file and return data based on date_filter
        """
        self.date_filter = date_filter
        start = time.time()
        self.data = dd.read_csv(finput)
        self.data = self.data[self.data[key_filter] == date_filter]
        end = time.time()
        print("Extract csv with dask:", round((end - start), 5), "sec")

    def remove_duplicate(self, keys=None) -> dd:
        """
        Remove duplicate record based on key(s)
        """
        start = time.time()

        if len(keys) > 1:
            s_key = "_".join(keys)
            self.data[s_key] = (
                self.data[keys[0]].astype(str)
                + "_"
                + self.data[keys[1]].astype(str)
            )
            self.data = self.data.drop_duplicates(
                subset="%s_%s" % (keys[0], keys[1])
            )
            self.data = self.data.drop("%s_%s" % (keys[0], keys[1]), axis=1)
        elif len(keys) == 1:
            self.data = self.data.drop_duplicates(subset=keys)
            end = time.time()

        end = time.time()
        print("Remove duplicate:", round((end - start), 5), "sec")
        self.data = self.data.compute()

    def __get_schema(self, fname):
        decimal_validation = CustomElementValidation(
            lambda d: check_decimal(d), "is not decimal"
        )
        int_validation = CustomElementValidation(
            lambda i: check_int(i), "is not integer"
        )
        date_validation = CustomElementValidation(
            lambda i: check_date(i),
            "is incorrect date string format. It should be YYYY-MM-DD",
        )
        time_validation = CustomElementValidation(
            lambda t: check_timeformat(t), "is not time"
        )
        null_validation = CustomElementValidation(
            lambda d: d is not np.nan, "this field cannot be null"
        )

        columns = list()
        with open(f"schema/{fname}.json", "r") as fin:
            for field in json.load(fin):
                validations = list()
                if field["type"] == "INT":
                    validations.append(int_validation)
                elif field["type"] == "DATE":
                    validations.append(date_validation)
                elif field["type"] == "DECIMAL":
                    validations.append(decimal_validation)
                elif field["type"] == "TIME":
                    validations.append(time_validation)

                if field["mode"] == "REQUIRED":
                    validations.append(null_validation)

                columns.append(Column(field["name"], validations))
        return pandas_schema.Schema(columns)

    def validate_data(self, fname=None):
        schema = self.__get_schema(fname)
        errors = schema.validate(self.data)
        if errors:
            raise ValueError("\n" + "\n".join([str(e) for e in errors]))
        return self.data


def run_timesheet_pipeline(csv_file=None, date_filter=None):
    print("Running pipeline timesheets")
    assert csv_file != None, "Missing argument required (csv_file)..."
    if date_filter:
        assert datetime.strptime(
            date_filter, "%Y-%m-%d"
        ), "Filter date not match with required format. Please input YYYY-MM-DD"

    pipeline = Pipeline()
    if date_filter:
        pipeline.read_data_based_key(
            f"../data/{csv_file}", "date", date_filter
        )
    else:
        pipeline.read_all_data(f"../data/{csv_file}")

    pipeline.remove_duplicate(["employee_id", "date"])
    if pipeline.data.empty:
        print("No data found...")
    else:
        final_data = pipeline.validate_data("timesheets")
        db_opr.insert_timesheet_record(final_data)
        print(f"Success with {len(final_data)} rows data")


def run_employee_pipeline(csv_file=None, date_filter=None):
    print("Running pipeline employee")
    assert csv_file != None, "Missing argument required (csv_file)..."
    if date_filter:
        assert datetime.strptime(
            date_filter, "%Y-%m-%d"
        ), "Filter date not match with required format. Please input YYYY-MM-DD"

    pipeline = Pipeline()

    if date_filter:
        pipeline.read_data_based_key(
            f"../data/{csv_file}", "join_date", date_filter
        )
    else:
        pipeline.read_all_data(f"../data/{csv_file}")

    pipeline.remove_duplicate(["employe_id"])
    if pipeline.data.empty:
        print("Pipeline stopped. No data found...")
    else:
        final_data = pipeline.validate_data("employee")
        db_opr.insert_employees_record(final_data)
        print(f"Success with {len(final_data)} rows data")


def main():
    yesterday = date.today() - timedelta(days=1)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csvfile",
        type=str,
        default="timesheets.csv",
        help="File to be uploaded to database",
    )
    parser.add_argument(
        "--datefilter",
        type=str,
        default=f"{yesterday}",
        help="Filter date. If timesheet, the script will be filtered by date and if employee will be filtered by join date  ",
    )
    args = parser.parse_args()

    if not (".csv" in args.csvfile):
        raise Exception("Source data didn't not found in folder 'data' !!!")

    if "timesheet" in args.csvfile:
        run_timesheet_pipeline(
            csv_file="timesheets.csv", date_filter=args.datefilter
        )
    elif "employee" in args.csvfile:
        run_employee_pipeline(
            csv_file="employees.csv", date_filter=args.datefilter
        )
    else:
        raise Exception(
            "Didn't match to any pipeline. Input employees or timesheets instead."
        )


if __name__ == "__main__":
    main()

# In[ ]:
