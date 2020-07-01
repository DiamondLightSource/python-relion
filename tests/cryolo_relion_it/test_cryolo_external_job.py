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

    @mock.patch("cryolo_external_job.os.system")
    def test_run_job(self, mock_os_system):
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
        block = mics_doc.add_new_block("")
        loop = block.init_loop("_rln", ["MicrographName", "CtfImage"])
        loop.add_row([mic_file, mic_file + "_ctf"])
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
        mock_os_system.assert_called_once_with(
            "cryolo_predict.py --conf config.json "
            f"-i {self.test_dir}/External/job002/cryolo_input "
            f"-o {self.test_dir}/External/job002/gen_pick "
            '--weights path/to/cryolo/gmodel.h5 --gpu "0 1" --threshold 0.3'
        )
        assert os.path.isfile("_manualpick.star")
        with open("_manualpick.star") as f:
            contents = f.read().strip()
            # TODO: fix this! Should be relative path, i.e. mic_star_file only, not abs path
            assert contents == os.path.join(self.test_dir, mic_star_file)

        assert os.path.isdir("picked_stars")
        assert os.path.isfile("Micrographs/micrograph_1_manualpick.star")
        assert len(os.listdir("picked_stars")) == 0


if __name__ == "__main__":
    unittest.main()
