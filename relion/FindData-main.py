import FindData
from pprint import pprint


# Input entries for the FindData class would be found once and entered here, then passed in as an argument when instantiating a FindData object.
# Currently part of the file path has to be included in the 'name' section, but this would ideally be sorted if we use this structre in the future.
# If this was the design used, something else to improve is the dictionary format
# - it can be hard to keep track of how many values are in each field, and it would feel more intuitive if each set of square brackets emcompassed one value and its corresponding fields.

input = {
    1: {
        "name": "MotionCorr/job002",
        1: [
            "total_motion",
            "star",
            "corrected_micrographs.star",
            1,
            "_rlnAccumMotionTotal",
        ],
        2: [
            "early_motion",
            "star",
            "corrected_micrographs.star",
            1,
            "_rlnAccumMotionEarly",
        ],
        3: [
            "late_motion",
            "star",
            "corrected_micrographs.star",
            1,
            "_rlnAccumMotionLate",
        ],
    },
    2: {
        "name": "CtfFind/job003",
        1: ["astigmatism", "star", "micrographs_ctf.star", 1, "_rlnCtfAstigmatism"],
        2: ["defocusU", "star", "micrographs_ctf.star", 1, "_rlnDefocusU"],
        3: ["defocusV", "star", "micrographs_ctf.star", 1, "_rlnDefocusV"],
        4: ["astigmatism_angle", "star", "micrographs_ctf.star", 1, "_rlnDefocusAngle"],
        5: [
            "max_resolution",
            "star",
            "micrographs_ctf.star",
            1,
            "_rlnCtfMaxResolution",
        ],
        6: [
            "cc/fig_of_merit",
            "star",
            "micrographs_ctf.star",
            1,
            "_rlnCtfFigureOfMerit",
        ],
        7: ["cc/fig_doesnt_exist", "star", "micrographs_ctf.star", 1, "_rlnCtfFigure"],
    },
}


input2 = {
    1: {
        "name": "MotionCorr/job002/Movies/GridSquare_24959318/Data",
        1: [
            "Align patch",
            "out",
            "FoilHole_24363955_Data_24996956_24996958_20200625_021313_fractions.out",
        ],
    }
}

first_data = FindData.FindData(
    "/dls/ebic/data/staff-scratch/ccpem/Relion31TutorialPrecalculatedResults", input,
)

second_data = FindData.FindData(
    "/dls/m02/data/2020/bi27053-1/processing/Relion_nd", input2
)

output = first_data.get_data()
pprint(output)

# pprint(second_data.get_data())
