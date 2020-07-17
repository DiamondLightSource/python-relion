import FindData

# Input entries for the FindData class would be found once and entered here, then passed in as an argument when instantiating a FindData object.
# Currently part of the file path has to be included in the 'name' section, but this would ideally be sorted if we use this structre in the future.
# If this was the design used, something else to improve is the dictionary format
# - it can be hard to keep track of how many values are in each field, and it would feel more intuitive if each set of square brackets emcompassed one value and its corresponding fields.

input = {
    1: {
        "name": "MotionCorr/job002",
        1: ["total_motion", "corrected_micrographs.star", 1, "_rlnAccumMotionTotal"],
    },
    2: {
        "name": "CtfFind/job003",
        1: ["astigmatism", "micrographs_ctf.star", 1, "_rlnCtfAstigmatism"],
        2: ["defocusU", "micrographs_ctf.star", 1, "_rlnDefocusU"],
        3: ["defocusV", "micrographs_ctf.star", 1, "_rlnDefocusV"],
    },
}

first_data = FindData.FindData(
    "/dls/ebic/data/staff-scratch/ccpem/Relion31TutorialPrecalculatedResults",
    "star",
    input,
)

output = first_data.get_data()
print(output)
print(output[0][1][1])
print(output[0][0][0])
print(output[1][1][1])
