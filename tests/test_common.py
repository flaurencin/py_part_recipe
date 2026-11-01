import pytest

from py_part_recipe.common import validate_handle


@pytest.mark.parametrize("handle", ("md0", "vg_root", "Volume-1", " padded ", 42))
def test_validate_handle_valid(handle):
    assert validate_handle(handle) == str(handle).strip()


@pytest.mark.parametrize(
    "handle", ("", "with space", "a/b", "a[b", "a\\b", "a^b", "a`b", "a.b")
)
def test_validate_handle_invalid(handle):
    with pytest.raises(ValueError):
        validate_handle(handle)
