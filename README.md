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

To run the test you should have loop devices available, loop100, loop101

To create then you can in your home favorite directory create two files the following way:

> ``` bash
>  dd if=/dev/zero of=./loop100 bs=4096 count=10000
>  dd if=/dev/zero of=./loop101 bs=4096 count=10000
>  sudo losetup /dev/loop100 ./loop100 
>  sudo losetup /dev/loop101 ./loop101
>  sudo chown $USER /dev/loop100
>  sudo chown $USER /dev/loop101
> ```