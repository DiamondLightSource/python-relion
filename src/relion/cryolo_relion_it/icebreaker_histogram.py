import gemmi
import json
import matplotlib.pyplot as plt
from pathlib import Path


def extract_ice_column(icebreaker_star_file_path):
    particles = gemmi.cif.read_file(icebreaker_star_file_path)
    data_as_dict = json.loads(particles.as_json())["particles"]
    ice_group = data_as_dict["_rlnhelicaltubeid"]
    return ice_group


def create_histogram(working_directory):
    icebreaker_path = str(working_directory / "External/Icebreaker_group/")
    print(icebreaker_path)
    if Path(icebreaker_path + "/particles.star").is_file():
        data = extract_ice_column(icebreaker_path + "/particles.star")
    else:
        return None
    plt.hist(x=data, bins="auto", rwidth=0.9)
    plt.xlabel("Relative estimated ice thickness")
    plt.ylabel("Number of particles")
    plt.title("Histogram of Icebreaker estimated ice thickness")
    plt.legend(["Total number of particles = " + str(len(data))])
    plt.show()
    plt.savefig(icebreaker_path + "/ice_hist.pdf")
