input_star = {
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
        4: [
            "astigmatism_angle",
            "star",
            "micrographs_ctf.star",
            1,
            "_rlnDefocusAngle",
        ],
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
        7: ["cc/fig_doesnt_exist", "star", "micrographs_ctf.star", 1, "_rlnCtfFigure",],
    },
}

input_out = {
    1: {
        "name": "MotionCorr/job002/Movies/GridSquare_24959318/Data",
        1: [
            "Align patch",
            "out",
            "FoilHole_24363955_Data_24996956_24996958_20200625_021313_fractions.out",
        ],
    }
}
