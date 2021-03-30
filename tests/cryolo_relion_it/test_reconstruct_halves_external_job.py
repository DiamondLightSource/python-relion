import json
import os
import shutil
import sys
import tempfile
import unittest

import gemmi

from relion_yolo_it import reconstruct_halves_external_job


class ReconstructHalvesExternalJobTest(unittest.TestCase):
    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    @unittest.mock.patch("relion_yolo_it.reconstruct_halves_external_job.subprocess")
    def test_main(self, mock_subprocess):
        # Prepare things
        config_file = "example_config.json"
        with open(config_file, "w") as f:
            json.dump({"model": {}}, f)

        mic_job_dir = "CtfFind/job001"
        mic_dir = os.path.join(mic_job_dir, "Micrographs")
        os.makedirs(mic_dir)
        mic_file = os.path.join(mic_dir, "micrograph_1.mrc")
        open(mic_file, "w").close()

        mic_star_file = os.path.join(mic_job_dir, "example_mics.star")
        mics_doc = gemmi.cif.Document()
        block = mics_doc.add_new_block("micrographs")
        loop = block.init_loop("_rln", ["MicrographName"])
        loop.add_row([mic_file])
        mics_doc.write_file(mic_star_file)

        ini_job_dir = "InitialModel/job002"
        os.makedirs(ini_job_dir)

        ini_star_file = os.path.join(ini_job_dir, "run_it300_data.star")
        ini_doc = gemmi.cif.Document()
        ini_block = ini_doc.add_new_block("data")
        ini_loop = ini_block.init_loop("_rln", ["ClassNumber"])
        ini_loop.add_row(["1"])
        ini_loop_02 = ini_block.init_loop("_rln", ["MicrographName"])
        ini_loop_02.add_row([mic_file])
        ini_doc.write_file(ini_star_file)

        select_job_dir = "External/job003"

        selsplit_star_dir = os.path.join(select_job_dir)
        os.makedirs(selsplit_star_dir)

        split_file_01 = os.path.join(selsplit_star_dir, "particles_class1_split1.star")
        open(split_file_01, "w").close()
        split_file_02 = os.path.join(selsplit_star_dir, "particles_class1_split2.star")
        open(split_file_02, "w").close()

        mask_diameter = 350

        job_dir = "External/job004"

        rec_halves_dir = os.path.join(job_dir)
        os.makedirs(rec_halves_dir)

        model_file_01 = os.path.join(job_dir, "3d_half1_model1.mrc")
        model_file_02 = os.path.join(job_dir, "3d_half2_model1.mrc")

        # Run the job
        orig_sys_argv = sys.argv[:]
        sys.argv[1:] = [
            "--out_dir",
            job_dir,
            "--in_mics",
            job_dir + "/particles_class1.star",
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

        call1 = unittest.mock.call(
            [
                "relion_reconstruct",
                "--i",
                split_file_01,
                "--o",
                model_file_01,
                "--ctf",
                "true",
                "--mask_diameter",
                f"{mask_diameter}",
            ],
            check=True,
        )

        call2 = unittest.mock.call(
            [
                "relion_reconstruct",
                "--i",
                split_file_02,
                "--o",
                model_file_02,
                "--ctf",
                "true",
                "--mask_diameter",
                f"{mask_diameter}",
            ],
            check=True,
        )

        mock_subprocess.run.assert_has_calls([call1, call2])

        assert os.path.isfile("RELION_JOB_EXIT_SUCCESS")

        # Restore state
        os.chdir(self.test_dir)
        sys.argv = orig_sys_argv


if __name__ == "__main__":
    unittest.main()
