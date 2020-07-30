input_star = {
    1: {
        "name": "MotionCorr",
        1: ["_rlnAccumMotionTotal", "corrected_micrographs.star", 1,],
        2: ["_rlnAccumMotionEarly", "corrected_micrographs.star", 1,],
        3: ["_rlnAccumMotionLate", "corrected_micrographs.star", 1,],
    },
    2: {
        "name": "CtfFind",
        1: ["_rlnCtfAstigmatism", "micrographs_ctf.star", 1,],
        2: ["_rlnDefocusU", "micrographs_ctf.star", 1,],
        3: ["_rlnDefocusV", "micrographs_ctf.star", 1,],
        4: ["_rlnDefocusAngle", "micrographs_ctf.star", 1,],
        5: ["_rlnCtfMaxResolution", "micrographs_ctf.star", 1,],
        6: ["_rlnCtfFigureOfMerit", "micrographs_ctf.star", 1,],
        7: ["_rlnCtfFigure", "micrographs_ctf.star", 1,],  # Doesn't exist
    },
    3: {
        "name": "Class2D",
        1: ["_rlnClassDistribution", "runmodel.star", 1,],
        2: ["_rlnAccuracyRotations", "runmodel.star", 1,],
        3: ["_rlnAccuracyTranslationsAngst", "runmodel.star", 1,],
        4: ["_rlnEstimatedResolution", "runmodel.star", 1,],
        5: ["_rlnOverallFourierCompleteness", "runmodel.star", 1,],
        6: ["_rlnClassNumber", "rundata.star", 1,],
    },
    4: {
        "name": "Class3D",
        1: ["_rlnClassDistribution", "runmodel.star", 1,],
        2: ["_rlnAccuracyRotations", "runmodel.star", 1,],
        3: ["_rlnAccuracyTranslationsAngst", "runmodel.star", 1,],
        4: ["_rlnEstimatedResolution", "runmodel.star", 1,],
        5: ["_rlnOverallFourierCompleteness", "runmodel.star", 1,],
    },
}

input_out = {
    1: {
        "name": "MotionCorr/job002/Movies/GridSquare_24959318/Data",
        1: [
            "Align patch",
            "FoilHole_24363955_Data_24996956_24996958_20200625_021313_fractions.out",
        ],
    }
}

input_class_number = {
    1: {"name": "Class2D", 1: ["_rlnClassNumber", "rundata.star", 1,]}
}
