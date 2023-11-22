#!/bin/bash

# set -x
# set -e
set -a

function display_service_status {
    local service_name="$1"
    local success="$2"
    local width=$(tput cols)

    green='\033[0;32m'
    red='\033[0;31m'
    reset='\033[0m'

    dots_padding=$((width - ${#service_name} - 9))  # 6 is the length of "[OK]" or "[FAILURE]"
    printf "%s" "$service_name"
    printf '%*s' $dots_padding | tr ' ' '.'
    if [ "$success" == "0" ]; then
        printf "${green}[OK]${reset}\n"
    else
        printf "${red}[FAILURE]${reset}\n"
        exit 1
    fi
}


echo -n > /tmp/pretest.log
packages=("util-linux" "mdadm" "lvm2" "python3" "dosfstools" "e2fsprogs" \
          "btrfs-progs" "zfsutils-linux" "ecryptfs-utils" "cifs-utils" \
          "hfsprogs" "xfsprogs" "exfatprogs" "reiserfsprogs" "libparted-dev"\
          "libparted-fs-resize0" "libparted2" )
for tool in ${packages[@]}
    do 
        pkg=$(dpkg -l | grep -Ec "^\s*ii  ${tool}(:\w+)* ")
        if [ "$pkg" == "0" ]; then
            printf "  adding package ${tool}\n"
            sudo apt install $tool -y 
        fi
        display_service_status "Package $tool installed" $?
    done

groups | grep -q '\bdisk\b'
    if [ "$?" != "0" ]; then
        printf "attemping to add the group to the user\n"
        sudo usermod -aG disk $USER
        group_added=0
    fi
    
display_service_status "User is member of disk group" $?

if [ ! -z $group_added ]; then
    echo run \'sg disk\' command and restart this script
    echo or logout and login again because you have been addes to the group disk
    exit 0
fi
for block_dev in loop100 loop101 loop102 loop103 loop104; do
    if [ ! -e "/dev/$block_dev" ]; then
    printf "building test block device /dev/$block_dev from /tmp/$block_dev\n"
    dd if=/dev/zero of=tmp/$block_dev bs=4096 count=10000 && \
    losetup /dev/$block_dev /tmp/$block_dev
    fi
done
display_service_status "Test Devices Created" $?


if [ ! -x ".venv" ]; then 
    python3 -m venv .venv
    display_service_status "Pyhton 3 venv creation" $?
fi

printf "\nLet\'s get sure you can run some commands using sudo with no password.\n"
test_cmds_count=$(sudo -l | grep -cE '\(ALL\) NOPASSWD: /usr/sbin/mdadm|\(ALL\) NOPASSWD: /usr/bin/partx')
if [ $test_cmds_count -lt 2 ]; then
    echo $USER 'ALL=(ALL) NOPASSWD:' $(which mdadm) | sudo tee /etc/sudoers.d/py_part_recipe > /dev/null && \
    echo $USER 'ALL=(ALL) NOPASSWD:' $(which partx) | sudo tee -a /etc/sudoers.d/py_part_recipe > /dev/null
fi
display_service_status "Privileged commands" $?


. ./.venv/bin/activate
display_service_status "Entering venv" $?
pip install --upgrade pip 2>&1 >> /tmp/pretest.log
display_service_status "Pip is up to date" $?
printf "\nLet\'s make poetry.\n"
pip install poetry 2>&1 >> /tmp/pretest.log && \
poetry install 2>&1 >> /tmp/pretest.log
display_service_status "Depency installation" $?
