from __future__ import annotations

import json
import os
import sys
from unittest import mock

import gemmi

from relion.cryolo_relion_it import mask_soft_edge_external_job


@mock.patch("relion.cryolo_relion_it.mask_soft_edge_external_job.subprocess")
def test_mask_soft_edge_main(mock_subprocess, tmpdir):
    # Prepare things
    os.chdir(tmpdir)
    config_file = tmpdir.join("example_config.json")
    with open(config_file, "w") as f:
        json.dump({"model": {}}, f)

    mic_job_dir = tmpdir.mkdir("CtfFind").mkdir("job001")
    mic_dir = mic_job_dir.mkdir("Micrographs")
    mic_file = mic_dir.join("micrograph_1.mrc")
    mic_file.write("")

    mic_star_file = mic_job_dir.join("example_mics.star")
    mics_doc = gemmi.cif.Document()
    block = mics_doc.add_new_block("micrographs")
    loop = block.init_loop("_rln", ["MicrographName"])
    loop.add_row([str(mic_file)])
    mics_doc.write_file(str(mic_star_file))

    ini_job_dir = tmpdir.mkdir("InitialModel").mkdir("job002")

    ini_star_file = ini_job_dir.join("run_it300_data.star")
    ini_doc = gemmi.cif.Document()
    ini_block = ini_doc.add_new_block("data")
    ini_loop = ini_block.init_loop("_rln", ["ClassNumber"])
    ini_loop.add_row(["1"])
    ini_loop_02 = ini_block.init_loop("_rln", ["MicrographName"])
    ini_loop_02.add_row([str(mic_file)])
    ini_doc.write_file(str(ini_star_file))

    box_size = 96
    angpix = 1
    outer_radius = 32

    job_dir = "External/job003"

    tmpdir.mkdir("External").mkdir("job003")

    # Run the job
    orig_sys_argv = sys.argv[:]
    sys.argv[1:] = [
        "--out_dir",
        job_dir,
        "--box_size",
        f"{box_size}",
        "--angpix",
        f"{angpix}",
        "--outer_radius",
        f"{outer_radius}",
    ]
    mask_soft_edge_external_job.main()

    call1 = mock.call(
        [
            "relion_mask_create",
            "--denovo",
            "true",
            "--box_size",
            f"{box_size}",
            "--angpix",
            f"{angpix}",
            "--outer_radius",
            f"{outer_radius}",
            "--width_soft_edge",
            "5",
        ],
        check=True,
    )

    mock_subprocess.run.assert_has_calls([call1])

    assert os.path.isfile(tmpdir / "External" / "job003" / "RELION_JOB_EXIT_SUCCESS")

    # Restore state
    sys.argv = orig_sys_argv
