import pytest
import relion


@pytest.fixture
def input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).class2D


def test_class_number(input):
    class2d_object = input
    assert class2d_object.class_number[0] == "24"


def test_class_distribution(input):
    class2d_object = input
    assert class2d_object.class_distribution[0] == "0.016487"


def test_output_is_serialisable(input):
    object = input
    assert object.class_number == eval(repr(object.class_number))


def test_all_keys_are_different(input):
    class2d_object = input
    class_dist = class2d_object.class_distribution
    accuracy_rot = class2d_object.accuracy_rotations
    accuracy_trans = class2d_object.accuracy_translations_angst
    estimated_res = class2d_object.estimated_resolution
    overall_fourier = class2d_object.overall_fourier_completeness
    reference_image = class2d_object.reference_image
    class2d_dict = class2d_object.construct_dict(
        reference_image,
        class_dist,
        accuracy_rot,
        accuracy_trans,
        estimated_res,
        overall_fourier,
    )

    # print(class2d_dict)
    key_list = list(class2d_dict.keys())
    for i in range(1, len(key_list) - 1):
        assert key_list[i] != key_list[i - 1]


def test_counter(input):
    class2d_object = input
    class_numbers = class2d_object.class_number
    class2d_object._count_all(class_numbers)


def test_percentage(input):
    class2d_object = input
    class_numbers = class2d_object.class_number
    percentage = class2d_object.percent_all_particles_per_class(class_numbers)
    print("Percent of particles from all data in each class:", percentage)
    print(
        "Percent of all particles included in the 20 most populated classes:",
        sum(x[1] for x in percentage),
    )
    assert percentage[0][1] == 18.327067669172934


def test_top_twenty_list(input):
    class2d_object = input
    class_numbers = class2d_object.class_number
    twenty_list = class2d_object.top_twenty_most_populated(class_numbers)
    print("20 most populated classes:", twenty_list)
    assert len(twenty_list) == 20
    assert twenty_list[0][0] == "16"


def test_twenty_sum(input):
    class2d_object = input
    class_numbers = class2d_object.class_number
    total = class2d_object._sum_top_twenty_particles(class_numbers)
    assert total == 8061


def test_sum_all(input):
    class2d_object = input
    class_numbers = class2d_object.class_number
    total = class2d_object._sum_all_particles(class_numbers)
    assert total == 10640


def test_percentage_top_twenty(input):
    class2d_object = input
    class_numbers = class2d_object.class_number
    percentages_of_twenty = class2d_object.percent_all_particles_in_top_twenty_classes(
        class_numbers
    )
    print(
        "Percent of the particles from the top twenty classes in each class:",
        percentages_of_twenty,
    )
    assert sum(x[1] for x in percentages_of_twenty) == 100
    assert len(percentages_of_twenty) == 20
