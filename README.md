<h2 align='center'> Assignment - Context Manager</h3>


# Assignment 
## Goal 1
Your first task is to create iterators for each of the four files that contained cleaned up data, of the correct type (e.g. string, int, date, etc), and represented by a named tuple.

For now these four iterators are just separate, independent iterators.


### Solution 

```
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
```

## Goal 2
Create a single iterable that combines all the columns from all the iterators.

The iterable should yield named tuples containing all the columns. Make sure that the SSN's across the files match!

All the files are guaranteed to be in SSN sort order, and every SSN is unique, and every SSN appears in every file.

Make sure the SSN is not repeated 4 times - one time per row is enough!

### Solution

The `MergedIterable` accepts the four iterators defined in the problem above, and merges them using the `ssn` key common to all four datasets. The `__next__` method in `MergedIterator` takes the `ssn` of the next `EmployeeIterator`, and queries a dictionary to get the relevant records from each of the other iterators. 


## Goal 3
Next, you want to identify any stale records, where stale simply means the record has not been updated since 3/1/2017 (e.g. last update date < 3/1/2017). Create an iterator that only contains current records (i.e. not stale) based on the last_updated field from the status_update file.

### Solution 
```
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
```

The `FilteredData` iterator takes the `MergedIterator` defined above, and returns all records which were last updated after 3rd January, 2017. 

## Goal 4
Find the largest group of car makes for each gender.

Possibly more than one such group per gender exists (equal sizes).