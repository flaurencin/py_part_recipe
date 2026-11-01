import pytest

from py_part_recipe import common
from py_part_recipe.common import gen_cmd_for_subprocess, validate_handle


@pytest.mark.parametrize("handle", ("md0", "vg_root", "Volume-1", " padded ", 42))
def test_validate_handle_valid(handle):
    assert validate_handle(handle) == str(handle).strip()


@pytest.mark.parametrize(
    "handle", ("", "with space", "a/b", "a[b", "a\\b", "a^b", "a`b", "a.b")
)
def test_validate_handle_invalid(handle):
    with pytest.raises(ValueError):
        validate_handle(handle)


@pytest.mark.parametrize("uid,prefix", ((0, []), (1000, ["sudo"])))
def test_gen_cmd_for_subprocess(monkeypatch, uid, prefix):
    monkeypatch.setattr(common, "UID", uid)
    assert gen_cmd_for_subprocess("mdadm --stop /dev/md0") == prefix + [
        "mdadm",
        "--stop",
        "/dev/md0",
    ]
    # a list is used as is, an argument with spaces is kept in one piece
    assert gen_cmd_for_subprocess(["mkfs.ext4", "-L", "my label", "/dev/x"]) == (
        prefix + ["mkfs.ext4", "-L", "my label", "/dev/x"]
    )
