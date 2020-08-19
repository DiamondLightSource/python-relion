import pytest
import relion
from pprint import pprint


@pytest.fixture
def mc_input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).motioncorrection


@pytest.fixture
def ctf_input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).ctffind


@pytest.fixture
def class2d_input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).class2D


@pytest.fixture
def class3d_input(dials_data):
    return relion.Project(dials_data("relion_tutorial_data")).class3D


def test_motion_correction_stage(mc_input):
    print("Motion Correction")
    mc_object = mc_input
    early_motion = mc_object.accum_motion_early
    late_motion = mc_object.accum_motion_late
    total_motion = mc_object.accum_motion_total
    jobs = mc_object.job_number
    names = mc_object.micrograph_name
    mc_dict = mc_object.construct_dict(
        jobs, names, total_motion, early_motion, late_motion
    )

    pprint(mc_dict)


def test_ctf_find_stage(ctf_input):
    print("CTF Find")
    ctf_object = ctf_input
    astigmatism = ctf_object.astigmatism
    defocus_u = ctf_object.defocus_u
    defocus_v = ctf_object.defocus_v
    defocus_angle = ctf_object.defocus_angle
    max_res = ctf_object.max_resolution
    fig_of_merit = ctf_object.fig_of_merit
    names = ctf_object.micrograph_name
    jobs = ctf_object.job_number
    ctf_dict = ctf_object.construct_dict(
        jobs,
        names,
        astigmatism,
        defocus_u,
        defocus_v,
        defocus_angle,
        max_res,
        fig_of_merit,

    )
    pprint(ctf_dict)


def test_class_2d_stage(class2d_input):
    print("Class 2D")
    class2d_object = class2d_input

    class_dist = class2d_object.class_distribution
    accuracy_rot = class2d_object.accuracy_rotations
    accuracy_trans = class2d_object.accuracy_translations_angst
    estimated_res = class2d_object.estimated_resolution
    overall_fourier = class2d_object.overall_fourier_completeness
    reference_image = class2d_object.reference_image

    # class_num_list = []
    # for item in reference_image:
    #    stripped = item.lstrip("0")
    #    splitted = stripped.split("@")
    #    class_num_list.append(splitted[0])

    class_numbers = class2d_object.class_number

    twenty_list = []
    for i in range(len(class_numbers)):
        twenty_list = class2d_object.top_twenty_most_populated(class_numbers[i])
        print("20 most populated classes:", twenty_list)

    twenty_class_nums = [twenty_list[i][0] for i in range(20)]
    job_nums = class2d_object.job_number
    print(twenty_class_nums)

    # class_dist_20 = [class_dist[int(item) - 1] for item in twenty_class_nums]
    # accuracy_rot_20 = [accuracy_rot[int(item) - 1] for item in twenty_class_nums]
    # accuracy_trans_20 = [accuracy_trans[int(item) - 1] for item in twenty_class_nums]
    # estimated_res_20 = [estimated_res[int(item) - 1] for item in twenty_class_nums]
    # overall_fourier_20 = [overall_fourier[int(item) - 1] for item in twenty_class_nums]
    # reference_image_20 = [reference_image[int(item) - 1] for item in twenty_class_nums]


    # print('ref', reference_image)
    # print(reference_image[96])
    # print('ref20', reference_image_20)

    # class2d_dict_20 = class2d_object.construct_dict(
    #    job_nums,
    #    reference_image_20,
    #    class_dist_20,
    #    accuracy_rot_20,
    #    accuracy_trans_20,
    #    estimated_res_20,
    #    overall_fourier_20,
    # )
    class2d_dict = class2d_object.construct_dict(
        job_nums,

        reference_image,
        class_dist,
        accuracy_rot,
        accuracy_trans,
        estimated_res,
        overall_fourier,
    )
    pprint(class2d_dict)
    # print(class2d_dict_20)



def test_class_3d_stage(class3d_input):
    print("Class 3D")
    class3d_object = class3d_input

    class_dist = class3d_object.class_distribution
    accuracy_rot = class3d_object.accuracy_rotations
    accuracy_trans = class3d_object.accuracy_translations_angst
    estimated_res = class3d_object.estimated_resolution
    overall_fourier = class3d_object.overall_fourier_completeness
    reference_image = class3d_object.reference_image
    job_nums = class3d_object.job_number
    class3d_dict = class3d_object.construct_dict(
        job_nums,

        reference_image,
        class_dist,
        accuracy_rot,
        accuracy_trans,
        estimated_res,
        overall_fourier,
    )
    pprint(class3d_dict)
