import json
import os
import sys
from unittest import mock

import gemmi

from relion.cryolo_relion_it import select_and_split_external_job


@mock.patch("relion.cryolo_relion_it.select_and_split_external_job.subprocess")
def test_select_and_split_main(mock_subprocess, tmpdir):
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

    ini_job_dir_name = "InitialModel/job002"
    ini_job_dir = tmpdir.mkdir("InitialModel").mkdir("job002")

    ini_star_file = ini_job_dir.join("run_it300_data.star")
    ini_doc = gemmi.cif.Document()
    ini_block = ini_doc.add_new_block("data")
    ini_loop = ini_block.init_loop("_rln", ["ClassNumber"])
    ini_loop.add_row(["1"])
    ini_loop_02 = ini_block.init_loop("_rln", ["MicrographName"])
    ini_loop_02.add_row([str(mic_file)])
    ini_doc.write_file(str(ini_star_file))

    job_dir = "External/job003"

    tmpdir.mkdir("External").mkdir("job003")

    # Run the job
    orig_sys_argv = sys.argv[:]
    sys.argv[1:] = [
        "--in_dir",
        ini_job_dir_name,
        "--out_dir",
        job_dir,
        "--in_mics",
        str(ini_star_file),
        "--outfile",
        "particles_class1.star",
        "--class_number",
        "1",
    ]

    select_and_split_external_job.main()

    call1 = mock.call(
        [
            "relion_star_handler",
            "--i",
            str(ini_star_file),
            "--o",
            os.path.join(job_dir, "particles_class1.star"),
            "--select",
            "rlnClassNumber",
            "1",
        ],
        check=True,
    )

    call2 = mock.call(
        [
            "relion_star_handler",
            "--i",
            os.path.join(job_dir, "particles_class1.star"),
            "--o",
            os.path.join(job_dir, "particles_class1.star"),
            "--split",
            "--random_order",
            "--nr_split",
            "2",
        ],
        check=True,
    )

    mock_subprocess.run.assert_has_calls([call1, call2])

    assert os.path.isfile(tmpdir / "External" / "job003" / "RELION_JOB_EXIT_SUCCESS")

    # Restore state
    sys.argv = orig_sys_argv
