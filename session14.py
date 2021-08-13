import csv
from collections import namedtuple, Counter
from datetime import datetime
from enum import Enum
from itertools import repeat
from functools import partial


def read_file(file_name):
    with open(file_name) as f:
        rows = csv.reader(f, delimiter=",", quotechar='"')
        yield from rows


class FieldFormat(Enum):
    INT = int
    STR = str
    DATE = partial(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S%z"))
    MODEL_YEAR = partial(lambda x: datetime.strptime(x, "%Y"))


def cast_row_values(zipped_obj) -> tuple:
    return (f.value(x) for f, x in zipped_obj)


def formatted_data(file_path: str, coltypes: list, tuple_name: str) -> int:
    """Returns a namedtuple after casting the content to their appropriate types"""
    rows = read_file(file_path)
    column_names = next(rows)
    car_details = namedtuple(tuple_name, column_names)
    yield from map(lambda x: car_details(*cast_row_values(zip(coltypes, x))), rows)


class EmployeeIterator:
    def __init__(self):
        self.employee = formatted_data(
            "Assignment/employment.csv", [FieldFormat.STR] * 4, "Employee"
        )

    def __next__(self):
        return next(self.employee)

    def __iter__(self):
        return self


class PersonalIterator:
    def __init__(self):
        self.personal_details = formatted_data(
            "Assignment/personal_info.csv", [FieldFormat.STR] * 5, "PersonalInfo"
        )

    def __next__(self):
        return next(self.personal_details)

    def __iter__(self):
        return self


class UpdateIterator:
    def __init__(self):
        self.update = formatted_data(
            "Assignment/update_status.csv",
            [FieldFormat.STR, *list(repeat(FieldFormat.DATE, 2))],
            "UpdateStatus",
        )

    def __next__(self):
        return next(self.update)

    def __iter__(self):
        return self


class VehiclesIterator:
    def __init__(self):
        self.vehicles = formatted_data(
            "Assignment/vehicles.csv",
            [*list(repeat(FieldFormat.STR, 3)), FieldFormat.MODEL_YEAR],
            "Vehicles",
        )

    def __next__(self):
        return next(self.vehicles)

    def __iter__(self):
        return self


class MergedIterable:
    def __init__(self):
        self.emp_it = EmployeeIterator()
        self.pif_it = PersonalIterator
        self.ups_it = UpdateIterator
        self.veh_it = VehiclesIterator
        self.ssn2pif = {p.ssn: p for p in self.pif_it()}
        self.ssn2ups = {u.ssn: u for u in self.ups_it()}
        self.ssn2veh = {v.ssn: v for v in self.veh_it()}

    def __iter__(self):
        return self.MergedIterator(
            self.emp_it, self.ssn2pif, self.ssn2ups, self.ssn2veh
        )

    class MergedIterator:
        def __init__(self, emp_it, ssn_pif, ssn_ups, ssn_veh):
            self.emp_it = emp_it
            self.ssn2pif = ssn_pif
            self.ssn2ups = ssn_ups
            self.ssn2veh = ssn_veh

            self.AllData = namedtuple(
                "AllData",
                [
                    "ssn",
                    "first_name",
                    "last_name",
                    "gender",
                    "language",
                    "employer",
                    "department",
                    "employee_id",
                    "vehicle_make",
                    "vehicle_model",
                    "model_year",
                    "created",
                    "last_updated",
                ],
            )

        def __next__(self):
            emp = next(self.emp_it)
            pinfo = self.ssn2pif[emp.ssn]
            ups = self.ssn2ups[emp.ssn]
            veh = self.ssn2veh[emp.ssn]
            return self.AllData(
                emp.ssn,
                pinfo.first_name,
                pinfo.last_name,
                pinfo.gender,
                pinfo.language,
                emp.employer,
                emp.department,
                emp.employee_id,
                veh.vehicle_make,
                veh.vehicle_model,
                veh.model_year,
                ups.created,
                ups.last_updated,
            )

        def __iter__(self):
            return self


class FilteredData:
    def __init__(self):
        self.iterator = MergedIterable()

    def __next__(self):
        while True:
            row = next(iter(self.iterator))
            if row.last_updated.replace(tzinfo=None) > datetime.strptime(
                "01/03/2017", "%m/%d/%Y"
            ):
                return row
            else:
                continue

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def count_largest_car_group(gender):
    with FilteredData() as it:
        filt_data = [x.vehicle_make for x in it if (x.gender == gender)]
    _count = Counter(filt_data)
    return {k: v for k, v in _count.items() if v == max(_count.values())}
