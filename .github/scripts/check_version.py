"""Fail unless a git tag is the version of the package: tag v1.2.3 for version 1.2.3.

    python .github/scripts/check_version.py v1.2.3 [pyproject.toml]

Used by the release workflow, so that a tag can not publish a wheel of another
version. Needs python 3.11 or newer (tomllib).
"""

import sys
from pathlib import Path

import tomllib

DEFAULT_PYPROJECT = Path(__file__).resolve().parents[2] / "pyproject.toml"


def main(argv: list[str]) -> int:
    if len(argv) not in (2, 3):
        print(__doc__, file=sys.stderr)
        return 2
    tag = argv[1]
    pyproject = Path(argv[2]) if len(argv) == 3 else DEFAULT_PYPROJECT
    version = tomllib.loads(pyproject.read_text(encoding="utf-8"))["project"]["version"]
    if tag != f"v{version}":
        print(
            f"error: the tag {tag} is not the version {version} of {pyproject.name}: "
            f"expected the tag v{version}",
            file=sys.stderr,
        )
        return 1
    print(f"the tag {tag} is the version {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
