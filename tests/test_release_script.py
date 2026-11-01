import subprocess
import sys
from pathlib import Path

import pytest
import tomllib

SCRIPT = Path(".github/scripts/check_version.py")
pytestmark = pytest.mark.skipif(
    sys.version_info < (3, 11), reason="the release workflow runs on python 3.12"
)


def check(*args) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args], capture_output=True, text=True
    )


def pyproject(tmp_path, version: str) -> str:
    path = tmp_path / "pyproject.toml"
    path.write_text(f'[project]\nname = "x"\nversion = "{version}"\n')
    return str(path)


def test_the_tag_is_the_version(tmp_path):
    result = check("v1.2.3", pyproject(tmp_path, "1.2.3"))
    assert result.returncode == 0
    assert "the tag v1.2.3 is the version 1.2.3" in result.stdout


@pytest.mark.parametrize("tag", ("v1.2.4", "1.2.3", "v1.2", "v1.2.3rc1", "V1.2.3", ""))
def test_the_tag_is_not_the_version(tmp_path, tag):
    result = check(tag, pyproject(tmp_path, "1.2.3"))
    assert result.returncode == 1
    assert "expected the tag v1.2.3" in result.stderr


def test_a_prerelease_tag_must_match_exactly(tmp_path):
    path = pyproject(tmp_path, "0.2.0rc1")
    assert check("v0.2.0rc1", path).returncode == 0
    assert check("v0.2.0", path).returncode == 1


@pytest.mark.parametrize("args", ([], ["v1", "a", "b"]))
def test_usage(args):
    result = check(*args)
    assert result.returncode == 2
    assert "check_version.py" in result.stderr


def test_the_tag_of_this_version_is_accepted():
    """The tag to push for the version in the repository passes the workflow's guard."""
    version = tomllib.loads(Path("pyproject.toml").read_text())["project"]["version"]
    assert check(f"v{version}").returncode == 0
    assert check(f"v{version}.1").returncode == 1
