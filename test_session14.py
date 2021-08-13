import session14
import pandas as pd


def test_employee_iterator():
    it = session14.EmployeeIterator()
    assert iter(it) == it and next(it).ssn == "100-53-9824"


def test_personal_iterator():
    it = session14.PersonalIterator()
    assert iter(it) == it and next(it).ssn == "100-53-9824"


def test_updated_iterator():
    it = session14.UpdateIterator()
    assert iter(it) == it and next(it).ssn == "100-53-9824"


def test_vechicle_iterator():
    it = session14.VehiclesIterator()
    assert iter(it) == it and next(it).ssn == "100-53-9824"


def test_merged_iterator():
    it = session14.MergedIterable()
    row = next(iter(it))
    assert row.ssn == "100-53-9824"
    assert row.vehicle_model == "Bravada"
    assert row.employer == "Stiedemann-Bailey"


def test_filtered_iterator():
    p = pd.read_csv("Assignment/update_status.csv")
    p["last_updated"] = pd.to_datetime(p["last_updated"]).dt.tz_localize(None)
    # Filtering records where last_updated happened after 3rd Jan, 2017
    num_records = p[p["last_updated"] > pd.Timestamp("2017-01-03")].shape[0]

    it = session14.FilteredData()
    total_records = len([x for x in it])

    assert total_records == num_records
