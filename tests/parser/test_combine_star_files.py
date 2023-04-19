from __future__ import annotations

from pathlib import Path
from unittest import mock

import pandas as pd

from relion._parser import combine_star_files


@mock.patch("relion._parser.combine_star_files.starfile")
def test_combine_star_files(mock_starfile, tmp_path):
    """Check that star files can be read in and a combine file written"""
    Path(tmp_path / "particles_split1.star").touch()
    Path(tmp_path / "particles_split2.star").touch()

    # set up a mock star file structure
    mock_starfile.read.return_value = {
        "optics": pd.DataFrame(data={"_column1": [1, 2], "_column2": [3, 4]}),
        "particles": pd.DataFrame(data={"_column1": [5, 6], "_column2": [7, 8]}),
    }

    combine_star_files.combine_star_files(
        [tmp_path / "particles_split1.star", tmp_path / "particles_split2.star"],
        tmp_path,
    )

    # assert that the particle files are read
    assert mock_starfile.read.call_count == 2
    mock_starfile.read.assert_any_call(tmp_path / "particles_split1.star")
    mock_starfile.read.assert_any_call(tmp_path / "particles_split2.star")

    # assert that the writing occurred with the correct data structures
    mock_starfile.write.assert_called_once()
    write_call = mock_starfile.write.call_args
    assert (
        (
            write_call[0][0]["optics"]
            == pd.DataFrame({"_column1": [1, 2], "_column2": [3, 4]})
        )
        .all()
        .all()
    )
    assert (
        (
            write_call[0][0]["particles"]
            == pd.DataFrame(
                {"_column1": [5, 6, 5, 6], "_column2": [7, 8, 7, 8]}, index=[0, 1, 0, 1]
            )
        )
        .all()
        .all()
    )
    assert write_call[0][1] == tmp_path / "particles_all.star"
    assert write_call[1] == {"overwrite": True}


@mock.patch("relion._parser.combine_star_files.starfile")
def test_split_star_file_n_splits(mock_starfile, tmp_path):
    """Test the star file splitting when given number of files to split into"""
    # set up a mock star file structure
    mock_starfile.read.return_value = {
        "optics": pd.DataFrame(data={"_column1": [1, 2], "_column2": [3, 4]}),
        "particles": pd.DataFrame(data={"_column1": [5, 6], "_column2": [7, 8]}),
    }

    combine_star_files.split_star_file(
        tmp_path / "particles_all.star", output_dir=tmp_path, number_of_splits=2
    )

    # assert that the particle files are read
    mock_starfile.read.assert_called_once()
    mock_starfile.read.assert_called_with(tmp_path / "particles_all.star")

    # assert that the writing occurred twice with the correct data structures
    assert mock_starfile.write.call_count == 2
    write_call = mock_starfile.write.call_args_list

    assert (
        (
            write_call[0][0][0]["optics"]
            == pd.DataFrame({"_column1": [1, 2], "_column2": [3, 4]})
        )
        .all()
        .all()
    )
    assert (
        (
            write_call[0][0][0]["particles"]
            == pd.DataFrame({"_column1": [5], "_column2": [7]}, index=[0])
        )
        .all()
        .all()
    )
    assert write_call[0][0][1] == tmp_path / "particles_split1.star"
    assert write_call[0][1] == {"overwrite": True}

    assert (
        (
            write_call[1][0][0]["optics"]
            == pd.DataFrame({"_column1": [1, 2], "_column2": [3, 4]})
        )
        .all()
        .all()
    )
    assert (
        (
            write_call[1][0][0]["particles"]
            == pd.DataFrame({"_column1": [6], "_column2": [8]}, index=[1])
        )
        .all()
        .all()
    )
    assert write_call[1][0][1] == tmp_path / "particles_split2.star"
    assert write_call[1][1] == {"overwrite": True}


@mock.patch("relion._parser.combine_star_files.starfile")
def test_split_star_file_n_particles(mock_starfile, tmp_path):
    """Test the star file splitting when given number of particles for each file"""
    # set up a mock star file structure
    mock_starfile.read.return_value = {
        "optics": pd.DataFrame(data={"_column1": [1, 2], "_column2": [3, 4]}),
        "particles": pd.DataFrame(data={"_column1": [5, 6], "_column2": [7, 8]}),
    }

    combine_star_files.split_star_file(
        tmp_path / "particles_all.star", output_dir=tmp_path, split_size=1
    )

    # assert that the particle files are read
    mock_starfile.read.assert_called_once()
    mock_starfile.read.assert_called_with(tmp_path / "particles_all.star")

    # assert that the writing occurred twice with the correct data structures
    assert mock_starfile.write.call_count == 2
    write_call = mock_starfile.write.call_args_list

    assert (
        (
            write_call[0][0][0]["optics"]
            == pd.DataFrame({"_column1": [1, 2], "_column2": [3, 4]})
        )
        .all()
        .all()
    )
    assert (
        (
            write_call[0][0][0]["particles"]
            == pd.DataFrame({"_column1": [5], "_column2": [7]}, index=[0])
        )
        .all()
        .all()
    )
    assert write_call[0][0][1] == tmp_path / "particles_split1.star"
    assert write_call[0][1] == {"overwrite": True}

    assert (
        (
            write_call[1][0][0]["optics"]
            == pd.DataFrame({"_column1": [1, 2], "_column2": [3, 4]})
        )
        .all()
        .all()
    )
    assert (
        (
            write_call[1][0][0]["particles"]
            == pd.DataFrame({"_column1": [6], "_column2": [8]}, index=[1])
        )
        .all()
        .all()
    )
    assert write_call[1][0][1] == tmp_path / "particles_split2.star"
    assert write_call[1][1] == {"overwrite": True}


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
