from relion.cryolo_relion_it import icebreaker_histogram
import os
import gemmi


def test_create_json_histogram(tmpdir):
    # Prep
    os.chdir(tmpdir)
    external_dir = tmpdir.mkdir("External")
    icebreaker_dir = external_dir.mkdir("Icebreaker_group")
    particles_file = icebreaker_dir.join("particles.star")
    particles_file.write("")

    assert icebreaker_histogram.create_json_histogram(tmpdir) == "ice_hist.json"


def test_create_json_histogram_fails_without_particle_star_file(tmpdir):
    # Prep
    os.chdir(tmpdir)

    assert icebreaker_histogram.create_json_histogram(tmpdir) is None


def test_create_json_histogram_creates_a_file(tmpdir):
    # Prep
    os.chdir(tmpdir)
    external_dir = tmpdir.mkdir("External")
    icebreaker_dir = external_dir.mkdir("Icebreaker_group")
    particles_file = icebreaker_dir.join("particles.star")
    out_doc = gemmi.cif.Document()
    output_nodes_block = out_doc.add_new_block("particles")
    loop = output_nodes_block.init_loop("", ["_rlnHelicalTubeID"])
    loop.add_row(["5"])
    out_doc.write_file(str(particles_file))

    icebreaker_histogram.create_json_histogram(tmpdir)
    assert os.path.isfile(tmpdir / "External" / "Icebreaker_group" / "ice_hist.json")


def test_create_pdf_histogram(tmpdir):
    # Prep
    os.chdir(tmpdir)
    external_dir = tmpdir.mkdir("External")
    icebreaker_dir = external_dir.mkdir("Icebreaker_group")
    particles_file = icebreaker_dir.join("particles.star")
    particles_file.write("")

    assert icebreaker_histogram.create_pdf_histogram(tmpdir) == "ice_hist.pdf"


def test_create_pdf_histogram_fails_without_particle_star_file(tmpdir):
    # Prep
    os.chdir(tmpdir)

    assert icebreaker_histogram.create_pdf_histogram(tmpdir) is None


def test_create_pdf_histogram_creates_a_file(tmpdir):
    # Prep
    os.chdir(tmpdir)
    external_dir = tmpdir.mkdir("External")
    icebreaker_dir = external_dir.mkdir("Icebreaker_group")
    particles_file = icebreaker_dir.join("particles.star")
    out_doc = gemmi.cif.Document()
    output_nodes_block = out_doc.add_new_block("particles")
    loop = output_nodes_block.init_loop("", ["_rlnHelicalTubeID"])
    loop.add_row(["5"])
    out_doc.write_file(str(particles_file))

    icebreaker_histogram.create_pdf_histogram(tmpdir)
    assert os.path.isfile(tmpdir / "External" / "Icebreaker_group" / "ice_hist.pdf")
