import pytest

from relion.dbmodel.modeltables import Table, pid


@pytest.fixture
def fake_table():
    columns = [
        "primary_id",
        "unique_value",
        "count",
        "comment",
        "appendable",
    ]
    return Table(
        columns,
        "primary_id",
        unique="unique_value",
        counters="count",
        append="appendable",
    )


@pytest.fixture
def fake_double_unique_table():
    columns = [
        "primary_id",
        "unique_value_01",
        "unique_value_02",
        "count",
        "comment",
        "appendable",
    ]
    return Table(
        columns,
        "primary_id",
        unique=["unique_value_01", "unique_value_02"],
        counters="count",
        append="appendable",
    )


def test_columns_correctly_initialised(fake_table):
    assert fake_table.columns == [
        "primary_id",
        "unique_value",
        "count",
        "comment",
        "appendable",
    ]
    assert fake_table._tab.get("primary_id") == []
    assert fake_table._tab.get("unique_value") == []
    assert fake_table._tab.get("count") == []
    assert fake_table._tab.get("comment") == []
    assert fake_table._primary_key == "primary_id"
    assert fake_table._unique == ["unique_value"]
    assert fake_table._counters == ["count"]


def test_adding_first_row(fake_table):
    pid.reset(1)
    fake_table.add_row({"unique_value": 1, "comment": "first insert"})
    for columns in fake_table._tab.values():
        assert len(columns) == 1
    assert fake_table._tab["primary_id"] == [1]
    assert fake_table._tab["unique_value"] == [1]
    assert fake_table._tab["count"] == [1]
    assert fake_table._tab["comment"] == ["first insert"]


def test_get_item(fake_table):
    pid.reset(1)
    fake_table.add_row({"unique_value": 1, "comment": "first insert"})
    assert fake_table["unique_value"] == [1]


def test_adding_second_new_row(fake_table):
    pid.reset(1)
    fake_table.add_row({"unique_value": 1, "comment": "first insert"})
    fake_table.add_row({"unique_value": 2, "comment": "second insert"})
    for columns in fake_table._tab.values():
        assert len(columns) == 2
    assert fake_table._tab["primary_id"] == [1, 2]
    assert fake_table._tab["unique_value"] == [1, 2]
    assert fake_table._tab["count"] == [1, 2]
    assert fake_table._tab["comment"] == ["first insert", "second insert"]


def test_adding_row_twice_returns_none(fake_table):
    pid.reset(1)
    fake_table.add_row({"unique_value": 1, "comment": "first insert"})
    pid_insert = fake_table.add_row({"unique_value": 1, "comment": "first insert"})
    for columns in fake_table._tab.values():
        assert len(columns) == 1
    assert fake_table._tab["primary_id"] == [1]
    assert fake_table._tab["unique_value"] == [1]
    assert fake_table._tab["count"] == [1]
    assert fake_table._tab["comment"] == ["first insert"]
    assert pid_insert is None


def test_adding_second_conflicting_row(fake_table):
    pid.reset(1)
    fake_table.add_row({"unique_value": 1, "comment": "first insert"})
    pid_insert = fake_table.add_row({"unique_value": 1, "comment": "new insert"})
    for columns in fake_table._tab.values():
        assert len(columns) == 1
    assert fake_table._tab["primary_id"] == [1]
    assert fake_table._tab["unique_value"] == [1]
    assert fake_table._tab["count"] == [1]
    assert fake_table._tab["comment"] == ["new insert"]
    assert pid_insert == 1


def test_appending(fake_table):
    pid.reset(1)
    fake_table.add_row({"unique_value": 1, "comment": "first insert", "appendable": 4})
    assert fake_table._tab["appendable"] == [4]
    pid_insert = fake_table.add_row(
        {"unique_value": 1, "comment": "first insert", "appendable": 5}
    )
    for columns in fake_table._tab.values():
        assert len(columns) == 1
    assert fake_table._tab["primary_id"] == [1]
    assert fake_table._tab["unique_value"] == [1]
    assert fake_table._tab["count"] == [1]
    assert fake_table._tab["comment"] == ["first insert"]
    assert fake_table._tab["appendable"] == [[4, 5]]
    assert pid_insert == 1


def test_appending_list_and_overlapping_list(fake_table):
    pid.reset(1)
    fake_table.add_row({"unique_value": 1, "comment": "first insert", "appendable": 4})
    assert fake_table._tab["appendable"] == [4]
    pid_insert = fake_table.add_row(
        {"unique_value": 1, "comment": "first insert", "appendable": [5, 6]}
    )
    for columns in fake_table._tab.values():
        assert len(columns) == 1
    assert fake_table._tab["primary_id"] == [1]
    assert fake_table._tab["unique_value"] == [1]
    assert fake_table._tab["count"] == [1]
    assert fake_table._tab["comment"] == ["first insert"]
    assert fake_table._tab["appendable"] == [[4, 5, 6]]
    assert pid_insert == 1
    pid_insert = fake_table.add_row(
        {"unique_value": 1, "comment": "first insert", "appendable": [4, 7, 8]}
    )
    assert fake_table._tab["appendable"] == [[4, 5, 6, 7, 8]]
    assert pid_insert == 1


def test_adding_rows_for_table_with_two_unique_columns(fake_double_unique_table):
    pid.reset(1)
    fake_double_unique_table.add_row({"unique_value_01": 1, "unique_value_02": 1})
    for columns in fake_double_unique_table._tab.values():
        assert len(columns) == 1
    fake_double_unique_table.add_row({"unique_value_01": 1, "unique_value_02": 2})
    for columns in fake_double_unique_table._tab.values():
        assert len(columns) == 2
    fake_double_unique_table.add_row({"unique_value_01": 2, "unique_value_02": 1})
    for columns in fake_double_unique_table._tab.values():
        assert len(columns) == 3
    pid_insert = fake_double_unique_table.add_row(
        {"unique_value_01": 1, "unique_value_02": 1}
    )
    assert pid_insert is None
    for columns in fake_double_unique_table._tab.values():
        assert len(columns) == 3


def test_get_row_index_for_single_index(fake_table):
    pid.reset(1)
    fake_table.add_row({"unique_value": 1, "comment": "first insert", "appendable": 4})
    index = fake_table.get_row_index("primary_id", 1)
    assert index == 0
    fake_table.add_row({"unique_value": 2, "comment": "second insert", "appendable": 4})
    index = fake_table.get_row_index("primary_id", 2)
    assert index == 1
    index = fake_table.get_row_index("unique_value", 1)
    assert index == 0


def test_get_row_index_multiple_indices(fake_table):
    pid.reset(1)
    fake_table.add_row({"unique_value": 1, "comment": "first insert", "appendable": 4})
    fake_table.add_row({"unique_value": 2, "comment": "second insert", "appendable": 4})
    indices = fake_table.get_row_index("appendable", 4)
    assert indices == [0, 1]


def test_get_row_index_not_present(fake_table):
    pid.reset(1)
    fake_table.add_row({"unique_value": 1, "comment": "first insert", "appendable": 4})
    index = fake_table.get_row_index("unique_value", 2)
    assert index is None


def test_get_row_by_primary_key(fake_table):
    pid.reset(1)
    fake_table.add_row({"unique_value": 1, "comment": "test", "appendable": 4})
    row = fake_table.get_row_by_primary_key(1)
    assert row["unique_value"] == 1
    assert row["appendable"] == 4
    assert row["count"] == 1
    assert row["comment"] == "test"
