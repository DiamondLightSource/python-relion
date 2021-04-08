import json
import os
import sys
from unittest import mock
import pathlib

import gemmi

from relion.cryolo_relion_it import cryolo_external_job


@mock.patch("relion.cryolo_relion_it.cryolo_external_job.subprocess")
def test_cryolo_external_job_main(mock_subprocess, tmpdir):
    # Prepare things
    os.chdir(tmpdir)
    config_file = tmpdir.join("example_config.json")
    with open(config_file, "w") as f:
        json.dump({"model": {}}, f)

    mic_job_dir = "CtfFind/job001"
    mic_job_dir = tmpdir.mkdir("CtfFind").mkdir("job001")
    mic_dir = mic_job_dir.mkdir("Micrographs")
    mic_file = mic_dir.join("micrograph_1.mrc")
    mic_file.write("")

    mic_star_file = mic_job_dir.join("example_mics.star")
    mics_doc = gemmi.cif.Document()
    block = mics_doc.add_new_block("micrographs")
    loop = block.init_loop("_rln", ["MicrographName"])
    loop.add_row(["CtfFind/job001/Micrographs/micrograph_1.mrc"])
    mics_doc.write_file(str(mic_star_file))

    job_dir = tmpdir.mkdir("External").mkdir("job002")

    cryolo_star_dir = job_dir.mkdir("gen_pick").mkdir("STAR")
    cryolo_file = cryolo_star_dir.join("micrograph_1.mrc")
    cryolo_file.write("")

    # Run the job
    orig_sys_argv = sys.argv[:]
    sys.argv[1:] = [
        "--o",
        "External/job002",
        "--pipeline_control",
        "External/job002",
        "--in_mics",
        "CtfFind/job001/example_mics.star",
        "--box_size",
        "256",
        "--threshold",
        "0.3",
        "--gmodel",
        "path/to/cryolo/gmodel.h5",
        "--config",
        str(tmpdir / "example_config.json"),
        "--gpu",
        '"0 1"',
        "--j",
        "10",
    ]
    cryolo_external_job.main()

    # Check results
    mock_subprocess.run.assert_called_once_with(
        [
            "cryolo_predict.py",
            "--conf",
            "config.json",
            "-i",
            f"{tmpdir}/External/job002/cryolo_input",
            "-o",
            f"{tmpdir}/External/job002/gen_pick",
            "--weights",
            "path/to/cryolo/gmodel.h5",
            "--gpu",
            '"0 1"',
            "--threshold",
            "0.3",
        ],
        check=True,
    )
    assert os.path.isfile(
        tmpdir / "External" / "job002" / "coords_suffix_autopick.star"
    )
    with open(tmpdir / "External" / "job002" / "coords_suffix_autopick.star") as f:
        contents = f.read().strip()
        assert contents == "CtfFind/job001/example_mics.star"

    assert os.path.isdir(tmpdir / "External" / "job002" / "picked_stars")
    assert os.path.isfile(
        tmpdir / "External" / "job002" / "Micrographs" / "micrograph_1_autopick.star"
    )
    assert len(os.listdir(tmpdir / "External" / "job002" / "picked_stars")) == 0

    assert os.path.isfile(tmpdir / "External" / "job002" / "RELION_OUTPUT_NODES.star")
    doc = gemmi.cif.read_file(
        str(tmpdir / "External" / "job002" / "RELION_OUTPUT_NODES.star")
    )
    block = doc.sole_block()
    table = block.find("_rlnPipeLineNode", ["Name", "Type"])
    assert len(table) == 1
    table_list = [str(pathlib.PurePosixPath(pathlib.Path(table[0][0]))), table[0][1]]
    assert table_list == ["External/job002/coords_suffix_autopick.star", "2"]

    # Restore state
    sys.argv = orig_sys_argv
