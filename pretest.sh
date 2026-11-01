#!/bin/bash

set -a

bold=$(tput bold)
green=$(tput setaf 10)
red=$(tput setaf 9)
purple=$(tput setaf 5)
reset=$(tput sgr0)

function display_service_status {
    local service_name="$1"
    local success="$2"
    local width
    # width=$(tput cols)
    width=60

    dots_padding=$((width - ${#service_name} - 9)) # 6 is the length of "[OK]" or "[FAILURE]"
    printf "%s" "$service_name"
    printf '%*s' $dots_padding | tr ' ' '.'
    if [ "$success" == "0" ]; then
        printf "%b" "${green}[OK]${reset}\n"
    else
        printf "%b" "${red}[FAILURE]${reset}\n"
        exit 1
    fi
}

function ensure_command {
    local -n commands=$1
    valid_paths=()
    for command in "${commands[@]}"; do
        command_path=$(command -v "$command")
        if [ -n "$command_path" ]; then
            valid_paths+=("$command_path")
        else
            printf "%s" "${purple}"
            echo "               $command: command not found" >&2
            printf "%s" "${reset}"
        fi
    done
}

echo -n >/tmp/pretest.log
packages=("util-linux" "mdadm" "lvm2" "python3" "dosfstools" "e2fsprogs"
    "btrfs-progs" "zfsutils-linux" "ecryptfs-utils" "cifs-utils"
    "hfsprogs" "xfsprogs" "exfatprogs" "reiserfsprogs" "libparted-dev"
    "libparted-fs-resize0" "libparted2" "apfsprogs")
for tool in "${packages[@]}"; do
    pkg=$(dpkg -l | grep -Ec "^\s*ii  ${tool}(:\w+)* ")
    if [ "$pkg" == "0" ]; then
        printf "%b" "  adding package ${tool}\n"
        sudo apt install "$tool" -y
    fi
    display_service_status "Package $tool installed" $?
done

if ! groups | grep -q '\bdisk\b'; then
    printf "%b" "attemping to add the group to the user\n"
    sudo usermod -aG disk "$USER"
    group_added=0
fi

display_service_status "User is member of disk group" $?

if [ -n "$group_added" ]; then
    echo run \'sg disk\' command and restart this script
    echo or logout and login again because you have been addes to the group disk
    exit 0
fi
for block_dev in loop100 loop101 loop102 loop103 loop104 loop105 loop106; do
    if [ ! -e "/dev/$block_dev" ]; then
        printf "%b" "building test block device /dev/$block_dev from /tmp/$block_dev\n"
        dd if=/dev/zero of=/tmp/$block_dev bs=4096 count=10000 &&
            losetup /dev/$block_dev /tmp/$block_dev
    fi
    display_service_status "Test Devices $block_dev Created" $?
done

if [ ! -x ".venv" ]; then
    python3 -m venv .venv
    display_service_status "Pyhton 3 venv creation" $?
fi

printf "%b" "\nLet's get sure you can run some commands using sudo\nwith no password.\n"
printf "%b" "${bold}  essentials :${reset} raid and partx\n"
cmds=("mdadm" "partx")
ensure_command cmds
raid_cmds=("${valid_paths[@]}")
printf "%b" "${bold}  Linux lvm  :${reset}
"
cmds=("pvs" "pvdisplay" "pvcreate" "vgcreate" "vgs" "vgdisplay" "lvcreate" "lvs" "lvdisplay" "pvremove" "vgremove" "lvremove")
ensure_command cmds
lvm_commands=("${valid_paths[@]}")
printf "%b" "${bold}  linux fs   :${reset} ext2 ext4 ext3 xfs
"
cmds=("mkfs.ext2" "mkfs.ext3" "mkfs.ext4" "mkfs.xfs")
ensure_command cmds
lnuxfs_commands=("${valid_paths[@]}")
printf "%b" "${bold}  windows fs :${reset} fat ntfs
"
cmds=("mkfs.fat" "mkfs.ntfs" "mkfs.exfat")
ensure_command cmds
winfs_commands=("${valid_paths[@]}")
printf "%b" "${bold}  macos fs   :${reset} hfs apfs
"
cmds=("mkfs.hfs" "mkapfs")
ensure_command cmds
applefs_commands=("${valid_paths[@]}")
printf "%b" "${bold}  other      :${reset} zfs btrfs
"
# shellcheck disable=SC2034
cmds=("mkfs.btrfs" "btrfs" "zfs" "zpool")
ensure_command cmds
otherfs_commands=("${valid_paths[@]}")

fs_commands=("${raid_cmds[@]}" "${otherfs_commands[@]}")
fs_commands=("${fs_commands[@]}" "${lvm_commands[@]}")
fs_commands=("${fs_commands[@]}" "${lnuxfs_commands[@]}")
fs_commands=("${fs_commands[@]}" "${winfs_commands[@]}")
fs_commands=("${fs_commands[@]}" "${applefs_commands[@]}")

echo -n >/tmp/sudo_py_part_recipe
for fscommand in "${fs_commands[@]}"; do
    echo "$USER ALL=(ALL) NOPASSWD: $fscommand" >>/tmp/sudo_py_part_recipe
done

sudo cp /tmp/sudo_py_part_recipe /etc/sudoers.d/py_part_recipe
display_service_status "Privileged commands" $?

# shellcheck disable=SC1091
source ./.venv/bin/activate
display_service_status "Entering venv" $?
pip install --upgrade pip >>/tmp/pretest.log 2>&1
display_service_status "Pip is up to date" $?
printf "%b" "\nLet's make poetry.\n"
pip install poetry >>/tmp/pretest.log 2>&1 &&
    poetry install >>/tmp/pretest.log 2>&1
display_service_status "Depency installation" $?
