import pytest
import relion
from pathlib import Path
from pprint import pprint


@pytest.fixture
def class2d(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).class2D


input = class2d


def test_list_all_jobs_in_class2d_directory(class2d):
    """
    When used in an iterator context the Class2D instance returns
    all known job and alias names.
    """
    assert sorted(class2d) == ["job008", "job013"]


def test_class2d_behaves_like_a_dictionary(class2d):
    """
    Class2D implements the Mapping abstract baseclass,
    in other words it behaves like a fancy dictionary.
    """
    dc = dict(class2d)
    assert list(dc) == list(class2d.keys()) == list(class2d)
    assert list(dc.values()) == list(class2d.values())


def test_job_num(input):
    class2d_object = input
    assert class2d_object.job_number[0] == "job008"


def test_class_number(input):
    class2d_object = input
    assert class2d_object.class_number[0][0] == "24"


def test_class_distribution(input):
    class2d_object = input
    assert class2d_object.class_distribution[0][0] == "0.016487"


def test_output_is_serialisable(input):
    object = input
    assert object.class_number == eval(repr(object.class_number))


def test_all_keys_are_different(input):
    class2d_object = input
    class2d_dict = class2d_object.construct_dict()

    pprint(class2d_dict)
    key_list = list(class2d_dict.keys())
    for i in range(1, len(key_list) - 1):
        assert key_list[i] != key_list[i - 1]


def test_percentage(input):
    class2d_object = input
    class_numbers = class2d_object.class_number
    job_numbers = class2d_object.job_number
    percentage = None
    for i in range(len(job_numbers)):
        percentage = class2d_object.percent_all_particles_per_class(class_numbers[i])
        print("Percent of particles from all data in each class:", percentage)
        print(
            "Percent of all particles included in the 20 most populated classes for",
            job_numbers[i],
            ":",
            sum(x[1] for x in percentage),
            "%",
        )

    assert percentage[0][1] == pytest.approx(19.94305)


def test_top_twenty_list(input):
    class2d_object = input
    class_numbers = class2d_object.class_number
    twenty_list = []

    for class_item in class_numbers:
        twenty_list = class2d_object.top_twenty_most_populated(class_item)

        print("20 most populated classes:", twenty_list)
    assert len(twenty_list) == 20
    assert twenty_list[0][0] == "16"  # this is the second list


def test_twenty_sum(input):
    class2d_object = input
    class_numbers = class2d_object.class_number
    job_numbers = class2d_object.job_number
    total = []
    for i in range(len(job_numbers)):
        total.append(class2d_object._sum_top_twenty_particles(class_numbers[i]))
        print(job_numbers[i], total[i], "particles")
    assert sum(total) == 8599


def test_sum_all(input):
    class2d_object = input
    class_numbers = class2d_object.class_number

    total = [
        class2d_object._sum_all_particles(class_item) for class_item in class_numbers
    ]

    assert sum(total) == 10640


def test_percentage_top_twenty_each(input):
    class2d_object = input
    class_numbers = class2d_object.class_number
    percentages_of_twenty = [
        class2d_object.percent_all_particles_in_top_twenty_classes(class_numbers[i])
        for i in range(len(class_numbers))
    ]

    for p in percentages_of_twenty:
        print(
            "Percent of the particles from the top twenty classes in each class:", p,
        )
    assert round(sum(x[1] for x in percentages_of_twenty[0]), 10) == 100
    assert round(sum(x[1] for x in percentages_of_twenty[1]), 10) == 100
    assert len(percentages_of_twenty[0]) == 20
    assert len(percentages_of_twenty[1]) == 20


@pytest.fixture
def bigger_data():
    return Path(
        "/dls/ebic/data/staff-scratch/colin/EMPIAR-10264/Refine3D"
    )  # /job005/run_it018_data.star')
