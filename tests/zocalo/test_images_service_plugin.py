from __future__ import annotations

import os
import pathlib
import sys
from typing import Any, Dict, NamedTuple, Protocol

import mrcfile
import pytest

import relion
from relion.zocalo.images_service_plugin import (
    mrc_central_slice,
    mrc_to_jpeg,
    picked_particles,
)

workflows = pytest.importorskip("workflows")


class _CallableParameter(Protocol):
    def __call__(self, key: str, default: Any = ...) -> Any:
        ...


class FunctionParameter(NamedTuple):
    rw: workflows.recipe.wrapper.RecipeWrapper
    parameters: _CallableParameter
    message: Dict[str, Any]


@pytest.fixture
def proj(dials_data):
    return relion.Project(dials_data("relion_tutorial_data", pathlib=True))


def plugin_params(jpeg_path):
    def params(key):
        p = {
            "parameters": {"images_command": "mrc_to_jpeg"},
            "file": jpeg_path.with_suffix(".mrc"),
        }
        return p.get(key)

    return FunctionParameter(rw=None, parameters=params, message={})


def plugin_params_central(jpeg_path):
    def params(key):
        p = {
            "parameters": {"images_command": "mrc_central_slice"},
            "file": jpeg_path.with_suffix(".mrc"),
        }
        return p.get(key)

    return FunctionParameter(rw=None, parameters=params, message={})


def plugin_params_parpick(jpeg_path, outfile):
    def params(key, default=None):
        p = {
            "parameters": {"images_command": "picked_particles"},
            "file": jpeg_path,
            "coordinates": [("0", "1"), ("2", "2")],
            "angpix": 0.5,
            "diameter": 190,
            "outfile": outfile,
        }
        return p.get(key) or default

    return FunctionParameter(rw=None, parameters=params, message={})


def test_contract_with_images_service():
    dlstbx_images = pytest.importorskip("dlstbx.services.images")

    # Check that we do not declare any keys that are unknown upstream
    assert set(FunctionParameter._fields).issubset(
        dlstbx_images.PluginInterface._fields
    )

    for key, annotation in FunctionParameter.__annotations__.items():
        if annotation is Any:
            continue
        upstream_type = dlstbx_images.PluginInterface.__annotations__[key]
        if annotation == upstream_type:
            continue
        if not hasattr(annotation, "_name") or not hasattr(upstream_type, "_name"):
            raise TypeError(
                f"Parameter {key!r} with local type {annotation!r} does not match upstream type {upstream_type!r}"
            )
        assert annotation._name == upstream_type._name


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_mrc_to_jpeg_nack_when_file_not_found(proj):
    ctf_data = proj.ctffind["job003"]
    jpeg_path = (
        proj.basepath
        / "CtfFind"
        / "job003"
        / "Movies"
        / "20170629_00021_frameImage_PS.jpeg"
    )
    assert ctf_data[0].diagnostic_plot_path == os.fspath(jpeg_path)

    assert not mrc_to_jpeg(plugin_params(jpeg_path))


def test_mrc_to_jpeg_ack_when_file_exists(tmp_path):
    np = pytest.importorskip("numpy")
    jpeg_path = tmp_path / "convert_test.jpeg"

    test_data = np.arange(9, dtype=np.int8).reshape(3, 3)
    with mrcfile.new(jpeg_path.with_suffix(".mrc")) as mrc:
        mrc.set_data(test_data)

    assert mrc_to_jpeg(plugin_params(jpeg_path)) == jpeg_path

    assert jpeg_path.is_file()


def test_picked_particles_processes_when_basefile_exists(tmp_path):
    np = pytest.importorskip("numpy")
    base_mrc_path = str(tmp_path / "base.mrc")
    out_jpeg_path = str(tmp_path / "processed.jpeg")
    test_data = np.arange(16, dtype=np.int8).reshape(4, 4)
    with mrcfile.new(base_mrc_path) as mrc:
        mrc.set_data(test_data)

    assert (
        picked_particles(plugin_params_parpick(base_mrc_path, out_jpeg_path))
        == out_jpeg_path
    )


def test_picked_particles_returns_None_when_basefile_does_not_exist(tmp_path):
    base_mrc_path = str(tmp_path / "base.mrc")
    out_jpeg_path = str(tmp_path / "processed.jpeg")

    assert not picked_particles(plugin_params_parpick(base_mrc_path, out_jpeg_path))


def test_central_slice_fails_with_2d(proj):
    micrograph_path = proj.motioncorrection["job002"][0].micrograph_name
    assert not mrc_central_slice(plugin_params_central(pathlib.Path(micrograph_path)))
