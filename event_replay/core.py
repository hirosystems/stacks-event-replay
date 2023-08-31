import gc
import json
import time
import numpy as np
import pandas as pd
import event_replay.helper as files_helper

from collections import deque
from event_replay.base_logger import logger

class CoreEventsProcessor:
    def __init__(self, tsv_path) -> None:
        logger.info('Stacks events reorganization into parquet files')

        self.tsv_path = tsv_path
        self.block_hashes = deque()
        self.burn_block_hashes = deque()
        self.find_last_block = True
        self.find_last_burn_block = True
        self.tsv_lines_count = 0
        self.block_canonical_count = 0
        self.block_orphan_count = 0
        self.last_block_height = -1
        self.burn_block_canonical_count = 0
        self.burn_block_orphan_count = 0
        self.last_burn_block_height = -1

    def get_microblock_hashes(self, block):
        microblocks = dict()
        for tx in block['transactions']:
            microblock_hash = tx['microblock_hash'] if tx['microblock_hash'] else ''
            microblock = microblocks.get(microblock_hash)
            if microblock:
                microblock['txs'].append(tx['txid'])
            else:
                microblocks.update({
                    microblock_hash: {
                        'microblock_sequence': tx['microblock_sequence'] if (tx['microblock_sequence'] or tx['microblock_sequence'] == 0) else -1,
                        'txs': [tx['txid']]
                    }
                })

        res = map(lambda x: [x[0], x[1]['txs']], [*microblocks.items()])
        return res

    def process_block(self, block):
        if self.find_last_block:
            self.block_hashes.extendleft([
                [block['index_block_hash'], self.get_microblock_hashes(block)],
                [block['parent_index_block_hash'], []]
            ])
            self.find_last_block = False
        else:
            if self.block_hashes[0][0] == block['index_block_hash']:
                self.block_hashes[0][1] = self.get_microblock_hashes(block)
                if block['block_height'] != 1:
                    self.block_hashes.extendleft([
                        [block['parent_index_block_hash'], []]
                    ])
                self.block_canonical_count += 1
                self.last_block_height = block['block_height']
            else:
                self.block_orphan_count += 1

    def process_burn_block(self, burn_block):
        if self.find_last_burn_block:
            self.burn_block_hashes.extendleft([burn_block['burn_block_hash']])
            self.find_last_burn_block = False
        else:
            if burn_block['burn_block_height'] >= self.last_burn_block_height:
                self.burn_block_orphan_count += 1
                return
            elif burn_block['burn_block_hash'] == self.burn_block_hashes[0]:
                self.burn_block_orphan_count += 1
                return
            else:
                self.burn_block_hashes.extendleft([burn_block['burn_block_hash']])
        self.last_burn_block_height = burn_block['burn_block_height']
        self.burn_block_canonical_count += 1

    def tsv_entity(self):
        logger.info('Extracting canonical indexes from TSV')
        df_reversed = pd.DataFrame()
        chunksize = 10000 # rows
        with pd.read_csv(
            self.tsv_path,
            compression='gzip',
            header=None,
            names=['id', 'timestamp', 'event', 'payload'],
            sep='\t',
            chunksize=chunksize) as reader:

            for chunk in reader:
                df_reversed = pd.concat([chunk.iloc[::-1], df_reversed], ignore_index=True)

        for i, row in df_reversed.iterrows():
            self.tsv_lines_count += 1
            event = row['event']
            if event == '/new_block':
                self.process_block(json.loads(row['payload']))
            elif event == '/new_burn_block':
                self.process_burn_block(json.loads(row['payload']))

        to_df = []
        for el in self.block_hashes:
            to_df.append([el[0], json.dumps(list(el[1]))])

        df = pd.DataFrame(to_df, columns=['index_block_hash', 'microblock'])
        files_helper.df_to_parquet("canonical/block_hashes", df)

        logger.info("TSV report")
        logger.info(f"---> File: {self.tsv_path}")
        logger.info(f"---> Lines count: {self.tsv_lines_count}")
        logger.info(f"---> NEW_BLOCK events canonical: {self.block_canonical_count}")
        logger.info(f"---> NEW_BLOCK events orphaned: {self.block_orphan_count}")
        logger.info(f"---> NEW_BURN_BLOCK events canonical: {self.burn_block_canonical_count}")
        logger.info(f"---> NEW_BURN_BLOCK events orphaned: {self.burn_block_orphan_count}")

        return df

    def prepare_dataframe(self) -> pd.DataFrame:
        logger.info('Augmenting TSV data')
        start_time = time.perf_counter()

        logger.info('---> reading TSV file')
        dataframe = pd.read_table(
            self.tsv_path,
            compression='gzip',
            header=None,
            names=['id', 'timestamp', 'event', 'payload']
        )

        # additional dataframe columns
        block_height = []
        index_block_hashes = []
        parent_index_block_hashes = []
        event_hashes = []

        logger.info('---> augmenting data with block_height, index_block_hash')
        for _, data in dataframe.iterrows():
            payload = json.loads(data['payload'])

            if data['event'] == '/new_block':
                block_height.append(str(payload['block_height']));
                index_block_hashes.append(payload['index_block_hash'])
                parent_index_block_hashes.append(payload['parent_index_block_hash'])
                event_hashes.append('/new_block')
            elif data['event'] == '/new_microblocks':
                block_height.append(np.nan);
                index_block_hashes.append('')
                parent_index_block_hashes.append(payload['parent_index_block_hash'])
                event_hashes.append('/new_microblocks')
            elif data['event'] == '/new_burn_block':
                block_height.append(np.nan);
                index_block_hashes.append('')
                parent_index_block_hashes.append('')
                event_hashes.append('/new_burn_block')
            elif data['event'] == '/new_mempool_tx':
                block_height.append(np.nan);
                index_block_hashes.append('')
                parent_index_block_hashes.append('')
                event_hashes.append('/new_mempool_tx')
            elif data['event'] == '/drop_mempool_tx':
                block_height.append(np.nan);
                index_block_hashes.append('')
                parent_index_block_hashes.append('')
                event_hashes.append('/drop_mempool_tx')
            else:
                block_height.append(np.nan);
                index_block_hashes.append('')
                parent_index_block_hashes.append('')
                event_hashes.append('/attachments/new')

        dataframe.insert(4, 'block_height', block_height)
        dataframe.insert(5, 'index_block_hash', index_block_hashes)
        dataframe.insert(6, 'parent_index_block_hash', parent_index_block_hashes)
        dataframe.insert(7, 'method', event_hashes)

        end_time = time.perf_counter()
        logger.info(f"---> finished in: {end_time - start_time:0.4f} seconds")

        gc.collect()

        return dataframe

    def split(self, df):
        logger.info(f"Splitting TSV dataframe")

        df_new_block = df[df.event == "/new_block"]

        # Limit of block_height to be replayed.
        # After this limit, events will be processed by regular HTTP POSTS.
        rewind_blocks = 256
        max_block_height = df_new_block['block_height'].dropna().astype(int).max()
        block_height_limit = str(max_block_height - rewind_blocks)

        # Split dataframe into two parts.
        # 1 - Events to be re-orged.
        # 2 - Remainder events to be replayed.
        row_index_limit = df_new_block.query('block_height == @block_height_limit').index.to_list()[0]
        df_core = df.iloc[:row_index_limit, :].copy(deep=False)
        df_remainder = df.iloc[row_index_limit:, :]

        logger.info(f"---> chain tip: {max_block_height}")
        logger.info(f"---> rewinding {rewind_blocks} blocks")
        logger.info(f"---> new chain tip: {block_height_limit}")

        return df_core, df_remainder

    def partition_prunable(self, df) -> None:
        """
        Create partitioned dataset from the dataframe.
        The partitions are based on the 'event' column.
        """

        logger.info('Partitioning prunable events')

        prunable_events = [
            "/drop_mempool_tx",
            "/new_mempool_tx",
            "/new_microblocks"
        ]

        for prunable in prunable_events:
            df_filtered = df.query('event == @prunable')
            files_helper.df_to_parquet(prunable, df_filtered)

    def remainder_to_parquet(self, df):
        logger.info('Creating parquet for remainder events')

        files_helper.df_to_parquet("remainder", df)
