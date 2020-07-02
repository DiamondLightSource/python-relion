input = {
    1: {
        "name": "MotionCorr",
        "values": [
            "total_motion",
            "corrected_micrographs.star",
            1,
            "_rlnAccumMotionTotal",
        ],
    }
}

print(input[1]["name"])
print(input[1]["values"][0])

new_input = {
    1: {
        "name": "MotionCorr",
        "values": ["total_motion"],
        "file_name": ["corrected_micrographs.star"],
        "block_number": [1],
        "loop_name": ["_rlnAccumMotionTotal"],
    },
    2: {
        "name": "CtfFind",
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

print("\n")
print(new_input[1]["name"])
print(new_input[1]["values"][0])
print(new_input[1]["block_number"][0])
print("here", len(new_input[1]["values"]))

for section in new_input:
    print(section)
