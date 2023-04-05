from __future__ import annotations

from relion.cryolo_relion_it.cryolo_relion_it import RelionItOptions


def test_relion_it_options_initialisation():
    options = RelionItOptions()
    assert options.particle_diameter == 0
    assert options.voltage == 200


def test_relion_it_options_particle_diameter_changes_propagate():
    options = RelionItOptions()
    assert options.extract_boxsize == 256
    options.particle_diameter = 300
    assert options.particle_diameter == 300
    assert options.extract_boxsize == 408
    assert options.extract_small_boxsize == 96
    options.particle_diameter = 200
    assert options.particle_diameter == 200
    assert options.extract_boxsize == 272
    assert options.extract_small_boxsize == 64


def test_relion_it_options_pixel_size_changes_propagate():
    options = RelionItOptions()
    assert options.angpix == 0.885
    options.particle_diameter = 300
    options.angpix = 1
    assert options.particle_diameter == 300
    assert options.extract_boxsize == 360
    assert options.extract_small_boxsize == 96
    options.particle_diameter = 200
    assert options.particle_diameter == 200
    assert options.extract_boxsize == 240
    assert options.extract_small_boxsize == 64


def test_relion_it_options_write_method(tmp_path):
    options = RelionItOptions()
    options.print_options(out_file=open(tmp_path / "relion_it_options.py", "w"))
    assert (tmp_path / "relion_it_options.py").exists()
    with open(tmp_path / "relion_it_options.py", "r") as f:
        lines = f.readlines()
    assert "particle_diameter = 0\n" in lines
    options.angpix = 1
    assert options.angpix == 1
    options.update_from_file(tmp_path / "relion_it_options.py")
    assert options.angpix == 0.885
