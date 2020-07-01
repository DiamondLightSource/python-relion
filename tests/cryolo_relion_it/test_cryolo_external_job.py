import json
import os
import shutil
import tempfile
import unittest
from unittest import mock

import gemmi

import cryolo_external_job


class CryoloExternalJobTest(unittest.TestCase):
    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    @mock.patch("cryolo_external_job.subprocess")
    def test_run_job(self, mock_subprocess):
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

        job_dir = "External/job002"
        os.makedirs(job_dir)
        os.chdir(job_dir)

        os.makedirs("gen_pick/STAR")
        open(os.path.join("gen_pick/STAR", os.path.basename(mic_file)), "w").close()

        # Run the job
        cryolo_external_job.run_job(
            self.test_dir,
            job_dir,
            [
                "--in_mics",
                mic_star_file,
                "--box_size",
                "256",
                "--threshold",
                "0.3",
                "--gmodel",
                "path/to/cryolo/gmodel.h5",
                "--config",
                os.path.join(self.test_dir, config_file),
                "--gpu",
                '"0 1"',
                "--j",
                "10",
            ],
        )

        # Check results
        mock_subprocess.run.assert_called_once_with(
            [
                "cryolo_predict.py",
                "--conf",
                "config.json",
                "-i",
                f"{self.test_dir}/External/job002/cryolo_input",
                "-o",
                f"{self.test_dir}/External/job002/gen_pick",
                "--weights",
                "path/to/cryolo/gmodel.h5",
                "--gpu",
                '"0 1"',
                "--threshold",
                "0.3",
            ],
            check=True,
        )
        assert os.path.isfile("_manualpick.star")
        with open("_manualpick.star") as f:
            contents = f.read().strip()
            assert contents == mic_star_file

        assert os.path.isdir("picked_stars")
        assert os.path.isfile("Micrographs/micrograph_1_manualpick.star")
        assert len(os.listdir("picked_stars")) == 0

        assert os.path.isfile("RELION_OUTPUT_NODES.star")
        doc = gemmi.cif.read_file("RELION_OUTPUT_NODES.star")
        block = doc.sole_block()
        table = block.find("_rlnPipeLineNode", ["Name", "Type"])
        assert len(table) == 1
        assert list(table[0]) == ["External/job002/_manualpick.star", "2"]


if __name__ == "__main__":
    unittest.main()
