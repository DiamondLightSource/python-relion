import unittest
import os
import tempfile
import gemmi
import sys
import shutil

from relion.cryolo_relion_it import fsc_fitting_external_job
from relion.cryolo_relion_it.cryolo_relion_it import findBestClassFSC


class FSCFittingExternalJobTest(unittest.TestCase):
    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_run_job_which_runs_linear_interpolation_on_star_file_data(self):

        pp_job_dir_01 = "PostProcess/GetFSC_1"
        os.makedirs(pp_job_dir_01)
        fsc_file_01 = os.path.join(pp_job_dir_01, "postprocess.star")
        open(fsc_file_01, "w").close()

        fsc_doc_01 = gemmi.cif.Document()
        block = fsc_doc_01.add_new_block("fsc")
        loop = block.init_loop("_rln", ["angstromresolution"])
        for x in [999, 100, 25, 10, 5, 2.5]:
            loop.add_row([str(x)])
        loop = block.init_loop("_rln", ["fouriershellcorrelationcorrected"])
        for fsc in [1, 0.7, 0.6, 0.45, 0.2, 0.001]:
            loop.add_row([str(fsc)])
        fsc_doc_01.write_file(fsc_file_01)

        pp_job_dir_02 = "PostProcess/GetFSC_2"
        os.makedirs(pp_job_dir_02)
        fsc_file_02 = os.path.join(pp_job_dir_02, "postprocess.star")
        open(fsc_file_02, "w").close()

        fsc_doc_02 = gemmi.cif.Document()
        block = fsc_doc_02.add_new_block("fsc")
        loop = block.init_loop("_rln", ["angstromresolution"])
        for x in [999, 100, 25, 10, 5, 2.5]:
            loop.add_row([str(x)])
        loop = block.init_loop("_rln", ["fouriershellcorrelationcorrected"])
        for fsc in [1, 0.65, 0.5, 0.2, 0.002, 0.0001]:
            loop.add_row([str(fsc)])
        fsc_doc_02.write_file(fsc_file_02)

        pp_job_dir_03 = "PostProcess/GetFSC_3"
        os.makedirs(pp_job_dir_03)
        fsc_file_03 = os.path.join(pp_job_dir_03, "postprocess.star")
        open(fsc_file_03, "w").close()

        fsc_doc_03 = gemmi.cif.Document()
        block = fsc_doc_03.add_new_block("fsc")
        loop = block.init_loop("_rln", ["angstromresolution"])
        for x in [999, 100, 25, 10, 5, 2.5]:
            loop.add_row([str(x)])
        loop = block.init_loop("_rln", ["fouriershellcorrelationcorrected"])
        for fsc in [1, 0.3, 0.65, 0.55, 0.2, 0.0001]:
            loop.add_row([str(fsc)])
        fsc_doc_03.write_file(fsc_file_03)

        job_dir = "External/FSCFitting"

        fsc_files = [
            "PostProcess/GetFSC_1/postprocess.star",
            "PostProcess/GetFSC_2/postprocess.star",
            "PostProcess/GetFSC_3/postprocess.star",
        ]
        best_class = fsc_fitting_external_job.run_job(
            os.getcwd(), job_dir, fsc_files, []
        )

        assert best_class == 1

    def test_cryolo_relion_it_findBestClassFSC(self):
        fsc_job_dir = "External/FSCFitting"
        os.makedirs(fsc_job_dir)
        best_class_file = os.path.join(fsc_job_dir, "BestClass.txt")
        with open(best_class_file, "w") as bcf:
            bcf.write("1")

        ini_job_dir = "InitialModel/job002"
        os.makedirs(ini_job_dir)

        ini_star_file = os.path.join(ini_job_dir, "run_it300_model.star")

        ref_file_01 = os.path.join(ini_job_dir, "run_it300_grad01.mrc")
        open(ref_file_01, "w").close()
        ref_file_02 = os.path.join(ini_job_dir, "run_it300_grad02.mrc")
        open(ref_file_02, "w").close()
        ini_doc = gemmi.cif.Document()
        ini_block = ini_doc.add_new_block("model_classes")
        ini_loop = ini_block.init_loop("_rln", ["ReferenceImage"])
        ini_loop.add_row([ref_file_01])
        ini_loop.add_row([ref_file_02])
        ini_loop_02 = ini_block.init_loop("_rln", ["EstimatedResolution"])
        ini_loop_02.add_row(["8.1"])
        ini_loop_02.add_row(["6.5"])

        # ref_file_02 = os.path.join(ini_job_dir, "run_it300_grad02.mrc")
        # open(ref_file_02, "w").close()
        # ini_loop.add_row([ref_file_02])
        # ini_loop_02.add_row(["6.5"])

        ini_block_02 = ini_doc.add_new_block("model_general")
        ini_loop_03 = ini_block_02.init_loop("_rln", ["PixelSize"])
        ini_loop_03.add_row(["0.8"])

        ini_doc.write_file(ini_star_file)

        (bclass, bresol, angpix) = findBestClassFSC(best_class_file, ini_star_file)

        assert bclass == ref_file_02
        assert bresol == 6.5
        assert angpix == ["0.8"]
