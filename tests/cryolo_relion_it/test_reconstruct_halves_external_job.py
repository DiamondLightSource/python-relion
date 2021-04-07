import json
import os
import sys
from unittest import mock

import gemmi

from relion.cryolo_relion_it import reconstruct_halves_external_job


@mock.patch("relion.cryolo_relion_it.reconstruct_halves_external_job.subprocess")
def test_reconstruct_halves_main(mock_subprocess, tmpdir):
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

    select_job_dir = "External/job003"
    selsplit_star_dir = tmpdir.mkdir("External").mkdir("job003")

    split_file_01_name = select_job_dir + "/particles_class1_split1.star"
    split_file_01 = selsplit_star_dir.join("particles_class1_split1.star")
    split_file_01.write("")
    split_file_02_name = select_job_dir + "/particles_class1_split2.star"
    split_file_02 = selsplit_star_dir.join("particles_class1_split2.star")
    split_file_02.write("")

    mask_diameter = 350

    job_dir_name = "External/job004"

    tmpdir.join("External").mkdir("job004")

    model_file_01_name = job_dir_name + "/3d_half1_model1.mrc"
    model_file_02_name = job_dir_name + "/3d_half2_model1.mrc"

    # Run the job
    orig_sys_argv = sys.argv[:]
    sys.argv[1:] = [
        "--out_dir",
        job_dir_name,
        "--in_mics",
        job_dir_name + "/particles_class1.star",
        "--in_dir",
        select_job_dir,
        "--i",
        "particles_class1.star",
        "--class_number",
        "1",
        "--mask_diameter",
        f"{mask_diameter}",
    ]
    reconstruct_halves_external_job.main()

    call1 = mock.call(
        [
            "relion_reconstruct",
            "--i",
            split_file_01_name,
            "--o",
            model_file_01_name,
            "--ctf",
            "true",
            "--mask_diameter",
            f"{mask_diameter}",
        ],
        check=True,
    )

    call2 = mock.call(
        [
            "relion_reconstruct",
            "--i",
            split_file_02_name,
            "--o",
            model_file_02_name,
            "--ctf",
            "true",
            "--mask_diameter",
            f"{mask_diameter}",
        ],
        check=True,
    )

    mock_subprocess.run.assert_has_calls([call1, call2])

    assert os.path.isfile(tmpdir / "External" / "job004" / "RELION_JOB_EXIT_SUCCESS")

    # Restore state
    sys.argv = orig_sys_argv
