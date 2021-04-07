import json
import os
import shutil
import sys
import tempfile
import unittest

import gemmi

from relion.cryolo_relion_it import select_and_split_external_job


class SelectAndSplitExternalJobTest(unittest.TestCase):
    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    @unittest.mock.patch(
        "relion.cryolo_relion_it.select_and_split_external_job.subprocess"
    )
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

        job_dir = "External/job003"

        selsplit_star_dir = os.path.join(job_dir)
        os.makedirs(selsplit_star_dir)

        # Run the job
        orig_sys_argv = sys.argv[:]
        sys.argv[1:] = [
            "--in_dir",
            ini_job_dir,
            "--out_dir",
            job_dir,
            "--in_mics",
            ini_star_file,
            "--outfile",
            "particles_class1.star",
            "--class_number",
            "1",
        ]

        select_and_split_external_job.main()

        call1 = unittest.mock.call(
            [
                "relion_star_handler",
                "--i",
                ini_star_file,
                "--o",
                os.path.join(job_dir, "particles_class1.star"),
                "--select",
                "rlnClassNumber",
                "1",
            ],
            check=True,
        )

        call2 = unittest.mock.call(
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

        assert os.path.isfile("RELION_JOB_EXIT_SUCCESS")

        # Restore state
        os.chdir(self.test_dir)
        sys.argv = orig_sys_argv


if __name__ == "__main__":
    unittest.main()
