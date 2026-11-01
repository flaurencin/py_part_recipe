import re
import os
import shlex
from typing import List, Union

HANDLE_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
UID = os.getuid()


def gen_cmd_for_subprocess(cmd: Union[str, List[str]]) -> List[str]:
    """Build the argv to run a command, prefixed with sudo when not root.

    Pass a list when an argument may contain spaces (for example a label),
    a string is split like a shell would do.
    """
    argv = shlex.split(cmd) if isinstance(cmd, str) else [str(a) for a in cmd]
    if UID != 0:
        argv = ["sudo"] + argv
    return argv


def validate_handle(handle: str) -> str:
    handle = str(handle).strip()
    if not HANDLE_RE.match(handle):
        raise ValueError("Handle string must match '^[a-zA-Z0-9_-]+$' pattern")
    return handle
