import pytest
import relion
import mrcfile
import sys
import os
import numpy as np
from relion.zocalo.images_service_plugin import mrc_to_jpeg
from typing import NamedTuple, Callable, Any, Dict


class FunctionParameter(NamedTuple):
    rw: Any
    parameters: Callable
    message: Dict


@pytest.fixture
def proj(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"))


def plugin_params(jpeg_path):
    def params(key):
        p = {
            "parameters": {"images_command": "mrc_to_jpeg"},
            "file": jpeg_path.with_suffix(".mrc"),
        }
        return p.get(key)

    return FunctionParameter(rw=None, parameters=params, message={})


def test_contract_with_images_service():
    dlstbx_images = pytest.importorskip("dlstbx.services.images")

    # Check that we do not declare any keys that are unknown upstream
    assert set(FunctionParameter._fields).issubset(
        dlstbx_images.PluginParameter._fields
    )

    for key, annotation in FunctionParameter.__annotations__.items():
        if annotation is Any:
            continue
        upstream_type = dlstbx_images.PluginParameter.__annotations__[key]
        if annotation == upstream_type:
            continue
        if not hasattr(annotation, "_name") or not hasattr(upstream_type, "_name"):
            raise TypeError(
                f"Upstream type {upstream_type} does not match local type {annotation} for parameter {key}"
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

    assert mrc_to_jpeg(plugin_params(jpeg_path)) is None


def test_mrc_to_jpeg_ack_when_file_exists(tmp_path):
    jpeg_path = tmp_path / "convert_test.jpeg"

    test_data = np.arange(9, dtype=np.int8).reshape(3, 3)
    with mrcfile.new(jpeg_path.with_suffix(".mrc")) as mrc:
        mrc.set_data(test_data)

    assert mrc_to_jpeg(plugin_params(jpeg_path)) == jpeg_path

    assert jpeg_path.is_file()
