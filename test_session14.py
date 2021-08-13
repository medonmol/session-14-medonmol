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
