# py_part_recipe
A partition manager based on yaml recipe for linux


## installing from source prerequisits

Ensure you have compilation tools available.
Ensure library liparted and libparted-devel are installed.

### For Debian based systems

You can run this command to ensure you have build tools

> ``` bash
> sudo apt-get install -y make build-essential libssl-dev zlib1g-dev \
> libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
> libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python-openssl
> ```

You can run that command to install libated and libparted-dev
> ``` bash
> sudo apt-get install -y libarted libparted-dev
> ```


## For Developpers based systems

### environment to run the tests

To run the test you should have loop devices available, loop100, loop101 and have packages etc.
to avoid the hustle to have to configure it all manually a script exists.
this script expects you to have temporary folder in /tmp and sudo privileges on you developpement workstation, under debian or ubuntu.

You miht be prompted for your sudo password to ensure package installation or file creations.

To set it all up you can run the pretest.sh script:

> ```
> prompt$ bash pretest.sh 
> Package util-linux installed.........................................[OK]
> Package mdadm installed..............................................[OK]
> Package lvm2 installed...............................................[OK]
> Package python3 installed............................................[OK]
> Package dosfstools installed.........................................[OK]
> Package e2fsprogs installed..........................................[OK]
> Package btrfs-progs installed........................................[OK]
> Package zfsutils-linux installed.....................................[OK]
> Package ecryptfs-utils installed.....................................[OK]
> Package cifs-utils installed.........................................[OK]
> Package hfsprogs installed...........................................[OK]
> Package xfsprogs installed...........................................[OK]
> Package exfatprogs installed.........................................[OK]
> Package reiserfsprogs installed......................................[OK]
> Package libparted-dev installed......................................[OK]
> Package libparted-fs-resize0 installed...............................[OK]
> Package libparted2 installed.........................................[OK]
> User is member of disk group.........................................[OK]
> Test Devices Created.................................................[OK]
> 
> Let's get sure you can run some commands using sudo with no password.
> Privileged commands..................................................[OK]
> Entering venv........................................................[OK]
> Pip is up to date....................................................[OK]
> 
> Let's make poetry.
> Depency installation.................................................[OK]
> ```