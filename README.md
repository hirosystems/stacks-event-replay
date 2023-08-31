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
$ curl -L https://archive.hiro.so/mainnet/stacks-blockchain-api/mainnet-stacks-blockchain-api-latest.gz -o ./mainnet-stacks-blockchain-api-latest.gz
```

2. Run the parquet generator using the TSV file as input

```shell
$ python3 -m event_replay --tsv-file mainnet-stacks-blockchain-api-latest.gz
```

An `events` folder is generated with a `dataset` that consists into subfolders and partitioned Parquet files for each event type present in the TSV file.

### Running the Events Ingestor

1. Run the events ingestion inside the [stacks-blockchain-api](https://github.com/hirosystems/stacks-blockchain-api) root folder.

```shell
$ STACKS_EVENTS_DIR="<EVENTS_FOLDER>" NODE_OPTIONS="--max-old-space-size=8192" STACKS_CHAIN_ID=<STACKS_CHAIN> node ./lib/index.js from-parquet-events --workers=<PARALLEL_WORKERS>
```

where:

`STACKS_EVENTS_DIR` is the path to the `events` folder.

`PARALLEL_WORKERS` is the number of workers that will run in parallel. Tests were done using a range between 4 and 8. Spawning several workers can lead to exhaustion of computational resources.

## REMARKS

**WARNING:** Running the event-replay will **wipe out** the stacks-blockchain-api postgres database. The event-replay is a process that deals with a huge ammount of data. Thus, is need a database in a mint state.
