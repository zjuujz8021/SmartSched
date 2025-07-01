## SmartSched
This repo contains the source code, dataset, and scripts for the paper "SmartSched: Enhancing Concurrent Smart Contract Execution via Instruction-Level Scheduling".

## Directory Structure

- The `./scripts` folder contains the scripts for running the benchmarks and figure plots.
- The execution results are stored in the `./results` folder.
- The `./build/bin` folder contains the built binary files.

## Environment

- Go 1.22
- Make >= 4.3
- Python3 >= 3.10
- Matplotlib >= 3.8

## Quick Start

Build the project:

```shell
make geth
make bench
```

You should see an output like `"Run xxx to launch xxx"` upon successful build.

## Evaluation

To reproduce the ERC-20 benchmark experiments:
```shell
./scripts/run_erc20.sh
```
This step may take several hours depending on your machine. Once completed, execution statistics are stored in the `./results/log/` directory.
To generate the corresponding figures, run:
```shell
python3 ./scripts/plot_erc20.py
```
The resulting plots for the ERC-20 benchmarks will be saved as PDF files in the `./results/` directory, with filenames matching the pattern `*erc20*.pdf`.

Running the history workload requires access to historical blockchain states, which are provided by a SlimArchive node. However, setting up and synchronizing a SlimArchive node is highly resource-intensive—it takes over one month and more than 1 TB of storage space.
Due to the complexity of this setup, we do not provide a reproduction pipeline for the history workload in this artifact.
Instead, we include the raw execution results used in the evaluation. These results are stored in:
```shell
./results/log/history_[thread_count].csv
```
Each CSV file contains per-block execution time under various methods.
To generate the figures related to the history workload, simply run:
```shell
python3 ./scripts/plot_history.py
```
The generated figures will be saved in the `./results/` directory, with filenames matching the pattern: `*history*.pdf`.