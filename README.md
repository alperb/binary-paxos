# binary-paxos

This implementation imitates remote paxos nodes reaching a consensus on a binary value (`0` or `1`). Implementation details can be found in `Homework.pdf`.

Required implementation details differ from my implementation. In the homework, all the necessary processes are described as standalone functions. In my implementation, described functions are wrapped inside a `Node` class.

## Installation
Clone the repo, _if not cloned_:
```bash
git clone https://github.com/alperb/binary-paxos.git
````

Create a virtual environment with the command:
```bash
python3 -m venv env
````

Change the source to virtual env:
```bash
source env/bin/activate
```

Install requirements:
```bash
pip install -r requirements.txt
```

## Run
Usage:
```
python paxos.py <numProc> <prob> <numRounds>
```

Example run:
```bash
python paxos.py 4 0.1 3
```