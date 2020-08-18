import pytest

import relion.EM_transform as EM


@pytest.fixture
def test_list():
    return [
        "Class2D",
        [
            "OverallFourierCompleteness/job013",
            "0.984734",
            "0.942004",
            "0.736861",
            "1.000000",
            "0.992732",
            "0.823452",
        ],
        [
            "ClassNumber/job008",
            "24",
            "4",
            "45",
            "28",
            "45",
            "40",
            "45",
            "45",
            "28",
            "24",
            "3",
        ],
        [
            "ClassNumber/job016",
            "24",
            "6",
            "65",
            "28",
            "45",
            "40",
            "65",
            "45",
            "28",
            "24",
            "6",
        ],
    ]


def test_initial(test_list):
    em_object = EM.EMTransform(test_list)
    em_object.group_by_class_number()


def test_particle_group_matching(test_list):
    em_object = EM.EMTransform(test_list)
    data = em_object.group_by_class_number()
    em_object.show_class_num_and_particle_count(data)


def test_counter(test_list):
    em_object = EM.EMTransform(test_list)
    em_object.try_counter()


def test_sort_by_particles(test_list):
    em_object = EM.EMTransform(test_list)
    data = em_object.group_by_class_number()
    em_object.sort_particles_per_class(data)


def test_sum_particles(test_list):
    em_object = EM.EMTransform(test_list)
    sum_list = em_object.sum_all_particles_per_section()
    assert sum_list[0][1] == 6
    assert sum_list[1][1] == 11
    assert sum_list[2][1] == 11


def test_percentage(test_list):
    em_object = EM.EMTransform(test_list)
    data = em_object.group_by_class_number()
    em_object.percent_all_particles_per_class(data)
