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
print("length", len(new_input[1]["values"]))

for section in new_input:
    print(section)

# This input format feels more intuitive, neater, and more compact
restructured_input = {
    1: {
        "name": "MotionCorr",
        1: ["total_motion", "corrected_micrographs.star", 1, "_rlnAccumMotionTotal"],
    },
    2: {
        "name": "CtfFind",
        1: ["astigmatism", "micrographs_ctf.star", 1, "_rlnCtfAstigmatism"],
        2: ["defocusU", "micrographs_ctf.star", 1, "_rlnDefocusU"],
        3: ["defocusV", "micrographs_ctf.star", 1, "_rlnDefocusV"],
    },
}

print(restructured_input[1]["name"])
print(restructured_input[1][1])
print(len(restructured_input[1]))
