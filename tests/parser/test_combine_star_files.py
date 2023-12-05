from __future__ import annotations

from unittest import mock

import starfile

from relion._parser import combine_star_files


@mock.patch("relion._parser.combine_star_files.Path.rename")
def test_combine_star_files(mock_rename, tmp_path):
    """Check that star files can be read in and a combine file written"""
    # set up a mock star file structure
    with open(tmp_path / "particles_split1.star", "w") as particles_all:
        particles_all.write(
            "data_optics\n\nloop_\n_column1\n_column2\nopticsGroup1 1\n\n"
            "data_particles\n\nloop_\n_column1\n_column2\n5 a\n6 b\n"
        )
    with open(tmp_path / "particles_split2.star", "w") as particles_all:
        particles_all.write(
            "data_optics\n\nloop_\n_column1\n_column2\nopticsGroup1 1\n\n"
            "data_particles\n\nloop_\n_column1\n_column2\n7 c\n8 d\n"
        )

    combine_star_files.combine_star_files(
        [tmp_path / "particles_split1.star", tmp_path / "particles_split2.star"],
        tmp_path,
    )

    # Check the split files have been made
    assert mock_rename.call_count == 1
    mock_rename.assert_called_with(tmp_path / "particles_all.star")

    assert (tmp_path / ".particles_all_tmp.star").exists()

    # Read in the star files that have been made
    starfile_data = starfile.read(tmp_path / ".particles_all_tmp.star")
    assert list(starfile_data.keys()) == ["optics", "particles"]
    assert len(starfile_data["optics"]) == 1
    assert list(starfile_data["optics"].loc[0]) == ["opticsGroup1", 1]
    assert len(starfile_data["particles"]) == 4
    assert list(starfile_data["particles"].loc[0]) == [5, "a"]
    assert list(starfile_data["particles"].loc[1]) == [6, "b"]
    assert list(starfile_data["particles"].loc[2]) == [7, "c"]
    assert list(starfile_data["particles"].loc[3]) == [8, "d"]


@mock.patch("relion._parser.combine_star_files.Path.rename")
def test_split_star_file_n_splits(mock_rename, tmp_path):
    """Test the star file splitting when given number of files to split into"""
    # set up a mock star file structure
    with open(tmp_path / "particles_all.star", "w") as particles_all:
        particles_all.write(
            "data_optics\n\nloop_\n_column1\n_column2\nopticsGroup1 1\n\n"
            "data_particles\n\nloop_\n_column1\n_column2\n5 a\n6 b\n"
        )

    combine_star_files.split_star_file(
        tmp_path / "particles_all.star", output_dir=tmp_path, number_of_splits=2
    )

    # Check the split files have been made
    assert mock_rename.call_count == 2
    mock_rename.assert_called_with(tmp_path / "particles_split2.star")

    assert (tmp_path / ".particles_split1_tmp.star").exists()
    assert (tmp_path / ".particles_split2_tmp.star").exists()

    # Read in the star files that have been made
    starfile_data = starfile.read(tmp_path / ".particles_split1_tmp.star")
    assert list(starfile_data.keys()) == ["optics", "particles"]
    assert len(starfile_data["optics"]) == 1
    assert list(starfile_data["optics"].loc[0]) == ["opticsGroup1", 1]
    assert len(starfile_data["particles"]) == 1
    assert list(starfile_data["particles"].loc[0]) == [5, "a"]

    starfile_data = starfile.read(tmp_path / ".particles_split2_tmp.star")
    assert list(starfile_data["particles"].loc[0]) == [6, "b"]


@mock.patch("relion._parser.combine_star_files.Path.rename")
def test_split_star_file_n_particles(mock_rename, tmp_path):
    """Test the star file splitting when given number of particles for each file"""
    # set up a mock star file structure
    with open(tmp_path / "particles_all.star", "w") as particles_all:
        particles_all.write(
            "data_optics\n\nloop_\n_column1\n_column2\nopticsGroup1 1\n\n"
            "data_particles\n\nloop_\n_column1\n_column2\n5 a\n6 b\n"
        )

    combine_star_files.split_star_file(
        tmp_path / "particles_all.star", output_dir=tmp_path, split_size=1
    )

    # Check the split files have been made
    assert mock_rename.call_count == 2
    mock_rename.assert_called_with(tmp_path / "particles_split2.star")

    assert (tmp_path / ".particles_split1_tmp.star").exists()
    assert (tmp_path / ".particles_split2_tmp.star").exists()

    # Read in the star files that have been made
    starfile_data = starfile.read(tmp_path / ".particles_split1_tmp.star")
    assert list(starfile_data.keys()) == ["optics", "particles"]
    assert len(starfile_data["optics"]) == 1
    assert list(starfile_data["optics"].loc[0]) == ["opticsGroup1", 1]
    assert len(starfile_data["particles"]) == 1
    assert list(starfile_data["particles"].loc[0]) == [5, "a"]

    starfile_data = starfile.read(tmp_path / ".particles_split2_tmp.star")
    assert list(starfile_data["particles"].loc[0]) == [6, "b"]


@mock.patch("relion._parser.combine_star_files.argparse._sys")
@mock.patch("relion._parser.combine_star_files.combine_star_files")
@mock.patch("relion._parser.combine_star_files.split_star_file")
def test_main(mock_split, mock_combine, mock_sysargv, tmp_path):
    # mock the values to be read in by the argument parser
    mock_sysargv.argv = [
        "command",
        str(tmp_path / "split1.star"),
        str(tmp_path / "split2.star"),
        "--output_dir",
        str(tmp_path),
        "--split",
        "--n_files",
        "2",
        "--split_size",
        "1",
    ]

    combine_star_files.main()

    # assert that this ran both the combining and splitting of star files
    mock_combine.assert_called_once()
    mock_combine.assert_called_with(
        [tmp_path / "split1.star", tmp_path / "split2.star"], tmp_path
    )
    mock_split.assert_called_once()
    mock_split.assert_called_with(tmp_path / "particles_all.star", tmp_path, 2, 1)
