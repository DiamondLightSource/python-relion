import pytest
import relion


@pytest.fixture
def input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).class3D


def test_class_number(input):
    class3d_object = input
    assert class3d_object.class_number[0][0] == "4"


def test_class_distribution(input):
    class3d_object = input
    assert class3d_object.class_distribution[0][0] == "0.055685"


def test_output_is_serialisable(input):
    object = input
    assert object.class_number == eval(repr(object.class_number))


def test_all_keys_are_different(input):
    class3d_object = input
    class_dist = class3d_object.class_distribution
    accuracy_rot = class3d_object.accuracy_rotations
    accuracy_trans = class3d_object.accuracy_translations_angst
    estimated_res = class3d_object.estimated_resolution
    overall_fourier = class3d_object.overall_fourier_completeness
    reference_image = class3d_object.reference_image
    job_num = class3d_object.job_number
    class3d_dict = class3d_object.construct_dict(
        job_num,
        reference_image,
        class_dist,
        accuracy_rot,
        accuracy_trans,
        estimated_res,
        overall_fourier,
    )

    # print(class3d_dict)
    key_list = list(class3d_dict.keys())
    for i in range(1, len(key_list) - 1):
        assert key_list[i] != key_list[i - 1]


def test_counter(input):
    class3d_object = input
    class_numbers = class3d_object.class_number
    for i in range(len(class_numbers)):
        class3d_object._count_all(class_numbers[i])


def test_percentage(input):
    class3d_object = input
    class_numbers = class3d_object.class_number
    job_numbers = class3d_object.job_number
    percentage = None
    for i in range(len(job_numbers)):
        percentage = class3d_object.percent_all_particles_per_class(class_numbers[i])
        print("Percent of particles from all data in each class:", percentage)
    assert percentage[0][1] == 83.86435625116452
    assert round(sum(x[1] for x in percentage), 10) == 100


def test_sum_all(input):
    class3d_object = input
    class_numbers = class3d_object.class_number
    total = []
    for i in range(len(class_numbers)):
        total.append(class3d_object._sum_all_particles(class_numbers[i]))
    assert sum(total) == 5367
