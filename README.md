# Stacks Event Replay

## Problem

The Stacks blockchain is only able to emit events live as they happen. This poses a problem in the scenario where the Stacks API needs to be upgraded and its database cannot be migrated to a new schema. For example, if there are breaking changes in the Stacks API's sql schema, like adding a new column which requires events to be replayed.

## Solution

One way to handle this upgrade is to wipe both the Stacks API's database and Stacks node working directory. But that approach would need a re-sync from scratch/genesis.

Alternatively, an event replay feature is possible where the Stacks API keeps stored the HTTP POST requests from the Stacks node event emitter, then streams these events back (replay) to itself. Essentially simulating a wipe & full re-sync, but much quicker.

The Stacks Event Replay is composed of 2 components: parquet generator and events ingestion.

## Installation

The Stacks Event Replay tooling is based on Python. Thus, make sure that you have Python instaled on your system before following the instructions below.

### Installing dependencies

```shell
$ make init
```

## Usage

### Running the Parquet Generator

1. Download a TSV file from the Hiro archive.

```shell
$ curl -L https://archive.hiro.so/testnet/stacks-blockchain-api/testnet-stacks-blockchain-api-latest.gz -o ./testnet-stacks-blockchain-api-latest.gz
```

2. Run the parquet generator using the TSV file as input

```shell
$ python3 -m parquet_generator --tsv-file testnet-stacks-blockchain-api-latest.gz
```

An `events` folder is generated to contain a `dataset` that consists into subfolders and partitioned Parquet files for each event present on the original TSV file.

### Running the Events Ingestor

TBD

