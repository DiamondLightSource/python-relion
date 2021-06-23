import pytest
import relion
import pathlib
import mrcfile
import sys
import numpy as np
from unittest import mock
from relion.zocalo.images_service_plugin import mrc_to_jpeg
from typing import NamedTuple
from workflows.transport.common_transport import CommonTransport


@pytest.fixture
def proj(dials_data):
    return relion.Project(dials_data("relion_tutorial_data"))


@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_mrc_to_jpeg_nack_when_file_not_found(proj):
    ctf_data = proj.ctffind["job003"]
    jpeg_path = str(
        pathlib.PurePosixPath(proj.basepath)
        / "CtfFind"
        / "job003"
        / "Movies"
        / "20170629_00021_frameImage_PS.jpeg"
    )
    assert ctf_data[0].diagnostic_plot_path == jpeg_path

    class RW_mock:
        def dummy(self, *args, **kwargs):
            pass

    ct = CommonTransport()
    ct.ack = mock.Mock()
    ct.nack = mock.Mock()

    rw = RW_mock()
    rw.transport = ct
    rw.recipe_step = {"parameters": {"images_command": "do_mrc_to_jpeg"}}
    rw.environment = {"has_recipe_wrapper": False}
    rw.set_default_channel = rw.dummy
    rw.send = rw.dummy

    msg = {"file": jpeg_path.replace(".jpeg", ".ctf")}
    header = {}

    class FunctionParameter(NamedTuple):
        rw: RW_mock
        message: dict
        header: dict

    params = FunctionParameter(rw, msg, header)

    mrc_to_jpeg(params)

    ct.nack.assert_called_once()
    ct.ack.assert_not_called()


def test_mrc_to_jpeg_ack_when_file_exists(tmp_path):
    jpeg_path = str(tmp_path / "convert_test.jpeg")

    test_data = np.arange(9, dtype=np.int8).reshape(3, 3)
    with mrcfile.new(jpeg_path.replace(".jpeg", ".mrc")) as mrc:
        mrc.set_data(test_data)

    class RW_mock:
        def dummy(self, *args, **kwargs):
            pass

    ct = CommonTransport()
    ct.ack = mock.Mock()
    ct.nack = mock.Mock()

    rw = RW_mock()
    rw.transport = ct
    rw.recipe_step = {"parameters": {"images_command": "do_mrc_to_jpeg"}}
    rw.environment = {"has_recipe_wrapper": False}
    rw.set_default_channel = rw.dummy
    rw.send = rw.dummy

    msg = {"file": jpeg_path.replace(".jpeg", ".mrc")}
    header = {}

    class FunctionParameter(NamedTuple):
        rw: RW_mock
        message: dict
        header: dict

    params = FunctionParameter(rw, msg, header)

    mrc_to_jpeg(params)

    ct.nack.assert_not_called()
    ct.ack.assert_called_once()

    assert pathlib.Path(jpeg_path).is_file()
