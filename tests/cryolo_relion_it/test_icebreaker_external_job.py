import gemmi

from relion.cryolo_relion_it import icebreaker_histogram


def test_create_json_histogram(tmp_path):
    # Prep
    icebreaker_dir = tmp_path / "External" / "Icebreaker_group"
    icebreaker_dir.mkdir(parents=True)
    particles_file = icebreaker_dir / "particles.star"
    particles_file.write_text("")

    assert icebreaker_histogram.create_json_histogram(tmp_path).name == "ice_hist.json"


def test_create_json_histogram_fails_without_particle_star_file(tmp_path):
    # Prep
    icebreaker_dir = tmp_path / "External" / "Icebreaker_group"
    icebreaker_dir.mkdir(parents=True)

    assert icebreaker_histogram.create_json_histogram(tmp_path) is None


def test_create_json_histogram_creates_a_file(tmp_path):
    # Prep
    icebreaker_dir = tmp_path / "External" / "Icebreaker_group"
    icebreaker_dir.mkdir(parents=True)
    particles_file = icebreaker_dir / "particles.star"

    out_doc = gemmi.cif.Document()
    output_nodes_block = out_doc.add_new_block("particles")
    loop = output_nodes_block.init_loop("", ["_rlnHelicalTubeID"])
    loop.add_row(["5"])
    out_doc.write_file(str(particles_file))

    icebreaker_histogram.create_json_histogram(tmp_path)
    assert icebreaker_dir.joinpath("ice_hist.json").is_file()


def test_create_pdf_histogram(tmp_path):
    # Prep
    icebreaker_dir = tmp_path / "External" / "Icebreaker_group"
    icebreaker_dir.mkdir(parents=True)
    particles_file = icebreaker_dir / "particles.star"
    particles_file.write_text("")

    assert icebreaker_histogram.create_pdf_histogram(tmp_path).name == "ice_hist.pdf"


def test_create_pdf_histogram_fails_without_particle_star_file(tmp_path):
    # Prep
    icebreaker_dir = tmp_path / "External" / "Icebreaker_group"
    icebreaker_dir.mkdir(parents=True)

    assert icebreaker_histogram.create_pdf_histogram(tmp_path) is None


def test_create_pdf_histogram_creates_a_file(tmp_path):
    # Prep
    icebreaker_dir = tmp_path / "External" / "Icebreaker_group"
    icebreaker_dir.mkdir(parents=True)
    particles_file = icebreaker_dir / "particles.star"
    out_doc = gemmi.cif.Document()
    output_nodes_block = out_doc.add_new_block("particles")
    loop = output_nodes_block.init_loop("", ["_rlnHelicalTubeID"])
    loop.add_row(["5"])
    out_doc.write_file(str(particles_file))

    icebreaker_histogram.create_pdf_histogram(tmp_path)
    assert icebreaker_dir.joinpath("ice_hist.pdf").is_file()
