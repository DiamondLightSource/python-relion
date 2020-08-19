import pytest
import relion


def test_basic_Project_object_behaviour(tmp_path):
    rp1 = relion.Project(tmp_path)
    assert rp1
    assert str(tmp_path) in str(rp1)
    assert tmp_path.name in repr(rp1)

    rp2 = relion.Project(str(tmp_path))
    assert rp2
    assert str(rp1) == str(rp2)
    assert repr(rp1) == repr(rp2)

    # check objects with equal paths are equal
    assert rp1 == rp2

    # ensure objects are hashable and equivalent
    assert len({rp1, rp2}) == 1


def test_create_Project_on_inaccessible_path_fails(tmp_path):
    with pytest.raises(ValueError):
        relion.Project(tmp_path / "does_not_exist")
