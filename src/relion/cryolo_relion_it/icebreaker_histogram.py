from __future__ import annotations

import json
import logging
import os

import gemmi
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px

logger = logging.getLogger("relion.cryolo_relion_it.icebreaker_histogram")


def create_json_histogram(working_directory):
    data = _get_data(working_directory)
    if data is None:
        return None

    df = pd.DataFrame(data, columns=["Relative estimated ice thickness"])
    fig = px.histogram(
        df,
        x="Relative estimated ice thickness",
        title="Histogram of Icebreaker estimated ice thickness <br>Total number of particles = "
        + str(len(data)),
    )
    full_json_path_object = (
        working_directory / "External" / "Icebreaker_group_batch_001" / "ice_hist.json"
    )
    fig.write_json(
        os.fspath(full_json_path_object)
    )  # This plotly version doesn't support Path objects with write_json()
    return full_json_path_object


def create_pdf_histogram(working_directory):
    data = _get_data(working_directory)
    if data is None:
        return None

    plt.hist(x=data, bins="auto", rwidth=0.9)
    plt.xlabel("Relative estimated ice thickness")
    plt.ylabel("Number of particles")
    plt.title("Histogram of Icebreaker estimated ice thickness")
    plt.legend(["Total number of particles = " + str(len(data))])
    full_pdf_path_object = (
        working_directory / "External" / "Icebreaker_group_batch_001" / "ice_hist.pdf"
    )
    plt.savefig(
        os.fspath(full_pdf_path_object)
    )  # This matplotlib version doesn't support Path objects with savefig()
    return full_pdf_path_object


def _get_data(working_directory):
    data = []
    icebreaker_paths = list((working_directory / "External").glob("Icebreaker_group*"))
    if not len(icebreaker_paths):
        return None
    passed = []
    for ibpath in icebreaker_paths:
        particles_file = ibpath / "particles.star"
        if particles_file.is_file():
            passed.append(True)
            current_data = extract_ice_column(particles_file)
            if not current_data:
                logger.debug(f"No ice thickness data extracted for {particles_file}.")
            else:
                data.extend(current_data)
        else:
            logger.debug(f"Icebreaker {particles_file} not found.")
            passed.append(False)

    if all(passed):
        return data

    return None


def extract_ice_column(icebreaker_star_file_path):
    particles = gemmi.cif.read_file(os.fspath(icebreaker_star_file_path))
    try:
        data_as_dict = json.loads(particles.as_json())["particles"]
        ice_group = data_as_dict["_rlnhelicaltubeid"]
        return ice_group
    except KeyError:
        return []
