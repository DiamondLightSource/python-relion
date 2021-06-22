import gemmi
import json
import matplotlib.pyplot as plt


def extract_ice_column(icebreaker_star_file_path):
    particles = gemmi.cif.read_file(icebreaker_star_file_path)
    data_as_dict = json.loads(particles.as_json())["particles"]
    print(data_as_dict)
    ice_group = data_as_dict["_rlnhelicaltubeid"]
    print(ice_group)
    return ice_group


def create_histogram(working_directory):
    icebreaker_path = str(working_directory / "External/Icebreaker_group/")
    try:
        data = extract_ice_column(icebreaker_path + "/particles.star")
        plt.hist(x=data, bins="auto", rwidth=0.9)
        plt.xlabel("Ice thickness")
        plt.ylabel("Number of particles")
        plt.title("Estimated ice thickness")
        plt.legend(["Total number of particles = " + str(len(data))])
        plt.show()
        plt.savefig(icebreaker_path + "/ice_hist.pdf")
    except Exception:
        print(
            "Error creating Icebreaker histogram",
        )
