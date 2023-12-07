import re
import os
import shlex
from typing import List

HANDLE_RE = re.compile(r"^[a-zA-z0-9_-]+$")
UID = os.getuid()


def gen_cmd_for_subprocess(cmd: str) -> List[str]:
    if UID != 0:
        cmd = f"sudo {cmd}"
    return shlex.split(cmd)


def validate_handle(handle: str) -> str:
    handle = str(handle).strip()
    if not HANDLE_RE.match(handle):
        raise ValueError("Handle string must match '^[a-zA-z0-9_-]+$' pattern")
    return handle
