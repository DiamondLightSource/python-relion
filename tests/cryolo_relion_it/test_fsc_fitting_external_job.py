import pytest
import gemmi
import sys

from relion.cryolo_relion_it import fsc_fitting_external_job


def test_run_job_which_runs_linear_interpolation_on_star_file_data(tmpdir):
    pp_job_dir = tmpdir.mkdir("PostProcess")
    fsc_file_01 = pp_job_dir.mkdir("GetFSC_1").join("postprocess.star")

    fsc_doc_01 = gemmi.cif.Document()
    block = fsc_doc_01.add_new_block("fsc")
    loop = block.init_loop("_rln", ["angstromresolution"])
    for x in [999, 100, 25, 10, 5, 2.5]:
        loop.add_row([str(x)])
    loop = block.init_loop("_rln", ["fouriershellcorrelationcorrected"])
    for fsc in [1, 0.7, 0.6, 0.45, 0.2, 0.001]:
        loop.add_row([str(fsc)])
    fsc_doc_01.write_file(str(fsc_file_01))

    fsc_file_02 = pp_job_dir.mkdir("GetFSC_2").join("postprocess.star")

    fsc_doc_02 = gemmi.cif.Document()
    block = fsc_doc_02.add_new_block("fsc")
    loop = block.init_loop("_rln", ["angstromresolution"])
    for x in [999, 100, 25, 10, 5, 2.5]:
        loop.add_row([str(x)])
    loop = block.init_loop("_rln", ["fouriershellcorrelationcorrected"])
    for fsc in [1, 0.65, 0.5, 0.2, 0.002, 0.0001]:
        loop.add_row([str(fsc)])
    fsc_doc_02.write_file(str(fsc_file_02))

    fsc_file_03 = pp_job_dir.mkdir("GetFSC_3").join("postprocess.star")

    fsc_doc_03 = gemmi.cif.Document()
    block = fsc_doc_03.add_new_block("fsc")
    loop = block.init_loop("_rln", ["angstromresolution"])
    for x in [999, 100, 25, 10, 5, 2.5]:
        loop.add_row([str(x)])
    loop = block.init_loop("_rln", ["fouriershellcorrelationcorrected"])
    for fsc in [1, 0.3, 0.65, 0.55, 0.2, 0.0001]:
        loop.add_row([str(fsc)])
    fsc_doc_03.write_file(str(fsc_file_03))

    job_dir = "External/FSCFitting"

    fsc_files = [
        "PostProcess/GetFSC_1/postprocess.star",
        "PostProcess/GetFSC_2/postprocess.star",
        "PostProcess/GetFSC_3/postprocess.star",
    ]
    best_class = fsc_fitting_external_job.run_job(tmpdir, job_dir, fsc_files, [])

    assert best_class == 1


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_cryolo_relion_it_findBestClassFSC(tmpdir):

    from relion.cryolo_relion_it.cryolo_relion_it import findBestClassFSC

    best_class_file = tmpdir.mkdir("External").mkdir("FSCFitting").join("BestClass.txt")
    best_class_file.write("1")

    ini_tmp_job_dir = tmpdir.mkdir("IntitialModel").mkdir("job002")

    ini_star_file = ini_tmp_job_dir.join("run_it300_model.star")

    ref_file_01 = ini_tmp_job_dir.join("run_it300_grad01.mrc")
    ref_file_02 = ini_tmp_job_dir.join("run_it300_grad02.mrc")
    ini_doc = gemmi.cif.Document()
    ini_block = ini_doc.add_new_block("model_classes")
    ini_loop = ini_block.init_loop("_rln", ["ReferenceImage"])
    ini_loop.add_row([str(ref_file_01)])
    ini_loop.add_row([str(ref_file_02)])
    ini_loop_02 = ini_block.init_loop("_rln", ["EstimatedResolution"])
    ini_loop_02.add_row(["8.1"])
    ini_loop_02.add_row(["6.5"])

    ini_block_02 = ini_doc.add_new_block("model_general")
    ini_loop_03 = ini_block_02.init_loop("_rln", ["PixelSize"])
    ini_loop_03.add_row(["0.8"])

    ini_doc.write_file(str(ini_star_file))

    (bclass, bresol, angpix) = findBestClassFSC(best_class_file, ini_star_file)

    assert bclass == ref_file_02
    assert bresol == 6.5
    assert angpix == ["0.8"]
