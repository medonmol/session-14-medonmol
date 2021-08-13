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


def test_counts():
    from rich import print

    vehicle_gender_data = pd.merge(
        pd.merge(
            pd.read_csv("Assignment/personal_info.csv"),
            pd.read_csv("Assignment/vehicles.csv"),
        ),
        pd.read_csv("Assignment/update_status.csv"),
    )
    vehicle_gender_data["last_updated"] = pd.to_datetime(
        vehicle_gender_data["last_updated"]
    ).dt.tz_localize(None)
    vehicle_gender_data = vehicle_gender_data[
        vehicle_gender_data["last_updated"] > pd.Timestamp("2017-01-03")
    ]

    agg_vehicle_make_by_gender = (
        vehicle_gender_data.groupby(["gender", "vehicle_make"])
        .size()
        .sort_values(ascending=True)
        .groupby(level=0)
        .tail(1)
        .reset_index(name="count")
    )

    vehicle_make_count_male = agg_vehicle_make_by_gender[
        agg_vehicle_make_by_gender["gender"] == "Male"
    ][["vehicle_make", "count"]].to_dict(orient="records")
    vehicle_make_count_female = agg_vehicle_make_by_gender[
        agg_vehicle_make_by_gender["gender"] == "Female"
    ][["vehicle_make", "count"]].to_dict(orient="records")

    agg_vehicle_make_by_gender_male = {
        item["vehicle_make"]: item["count"] for item in vehicle_make_count_male
    }

    agg_vehicle_make_by_gender_female = {
        item["vehicle_make"]: item["count"] for item in vehicle_make_count_female
    }

    assert agg_vehicle_make_by_gender_male == session14.count_largest_car_group("Male")
    assert agg_vehicle_make_by_gender_female == session14.count_largest_car_group(
        "Female"
    )
