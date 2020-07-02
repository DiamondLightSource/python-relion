import FindData

# Input entries for the FindData class would be found once and entered here, then passed in as an argument when instantiating a FindData object.
# Currently part of the file path has to be included in the 'name' section, but this would ideally be sorted if we use this structre in the future.
# If this was the design used, something else to improve is the dictionary format
# - it can be hard to keep track of how many values are in each field, and it would feel more intuitive if each set of square brackets emcompassed one value and its corresponding fields.

input = {
    1: {
        "name": "MotionCorr/relioncor2",
        "values": ["total_motion"],
        "file_name": ["corrected_micrographs.star"],
        "block_number": [1],
        "loop_name": ["_rlnAccumMotionTotal"],
    },
    2: {
        "name": "CtfFind/job003",
        "values": ["astigmatism", "defocusU", "defocusV"],
        "file_name": [
            "micrographs_ctf.star",
            "micrographs_ctf.star",
            "micrographs_ctf.star",
        ],
        "block_number": [1, 1, 1],
        "loop_name": ["_rlnCtfAstigmatism", "_rlnDefocusU", "_rlnDefocusV"],
    },
}


first_data = FindData.FindData(
    "/dls/ebic/data/staff-scratch/ccpem/Relion31TutorialPrecalculatedResults",
    "star",
    input,
)
first_data.get_data()
