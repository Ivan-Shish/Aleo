# Setup 1 scripts

Some scripts to help with running & testing the setup

## Usage

```bash
# Make working directory and clone the projects
mk workdir
cd workdir
clone aleo-setup-coordinator
clone aleo-setup

# Copy the scripts to working directory
cp aleo-setup/setup1-scripts/*.sh .

# Build the projects
./build_all.sh

# Run the setup
./run_setup.sh
```

## Log files

As the setup will run several processes in background,
the stdin & stdout of these processes will be written to log files
in the working directory. Use `tail -f file_name.txt` in order to read
the logs continuously.

Currently there are

```
coordinator_logs.txt
contributor_logs.txt
verifier_logs.txt
```
