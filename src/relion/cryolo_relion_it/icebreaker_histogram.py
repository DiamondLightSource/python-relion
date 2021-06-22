import gemmi
import json
import matplotlib.pyplot as plt
from pathlib import Path
import plotly.express as px
import pandas as pd


def create_json_histogram(working_directory):
    icebreaker_path = str(working_directory / "External/Icebreaker_group/")
    json_hist_file_name = "ice_hist.json"
    if Path(icebreaker_path + "/particles.star").is_file():
        data = extract_ice_column(icebreaker_path + "/particles.star")
    else:
        return None
    if data is None:
        print("No data extracted.")
    df = pd.DataFrame(data, columns=["Relative estimated ice thickness"])
    fig = px.histogram(
        df,
        x="Relative estimated ice thickness",
        title="Histogram of Icebreaker estimated ice thickness <br>Total number of particles = "
        + str(len(data)),
    )
    fig.write_json(icebreaker_path + "/" + json_hist_file_name)
    return json_hist_file_name


def create_pdf_histogram(working_directory):
    icebreaker_path = str(working_directory / "External/Icebreaker_group/")
    pdf_hist_file_name = "ice_hist.pdf"
    if Path(icebreaker_path + "/particles.star").is_file():
        data = extract_ice_column(icebreaker_path + "/particles.star")
    else:
        return None
    plt.hist(x=data, bins="auto", rwidth=0.9)
    plt.xlabel("Relative estimated ice thickness")
    plt.ylabel("Number of particles")
    plt.title("Histogram of Icebreaker estimated ice thickness")
    plt.legend(["Total number of particles = " + str(len(data))])
    plt.savefig(icebreaker_path + "/" + pdf_hist_file_name)
    return pdf_hist_file_name


def extract_ice_column(icebreaker_star_file_path):
    particles = gemmi.cif.read_file(icebreaker_star_file_path)
    try:
        data_as_dict = json.loads(particles.as_json())["particles"]
        ice_group = data_as_dict["_rlnhelicaltubeid"]
        return ice_group
    except KeyError:
        return []
