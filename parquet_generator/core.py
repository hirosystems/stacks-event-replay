import logging
import json
import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

from collections import deque
from pprint import pprint

logging.basicConfig(format='%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

class CoreEventsProcessor:
    def __init__(self, tsv_path) -> None:
        logger.info('[stacks-event-replay] Stacks events reorganization into parquet files')

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
        df_reversed = pd.DataFrame()
        chunksize = 10 ** 6 # 1M size
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
        table = pa.Table.from_pandas(df)
        ds.write_dataset(
            table,
            base_dir='events/canonical/block_hashes',
            format='parquet',
            existing_data_behavior='overwrite_or_ignore'
        )

        logger.info("[stacks-event-replay] ---| TSV file analysis |---")
        logger.info(f"[stacks-event-replay] File: {self.tsv_path}")
        logger.info(f"[stacks-event-replay] Lines count: {self.tsv_lines_count}")
        logger.info(f"[stacks-event-replay] NEW_BLOCK events canonical: {self.block_canonical_count}")
        logger.info(f"[stacks-event-replay] NEW_BLOCK events orphaned: {self.block_orphan_count}")
        logger.info(f"[stacks-event-replay] NEW_BURN_BLOCK events canonical: {self.burn_block_canonical_count}")
        logger.info(f"[stacks-event-replay] NEW_BURN_BLOCK events orphaned: {self.burn_block_orphan_count}")

    def prepare_dataframe(self) -> pd.DataFrame:
        logger.info('[stacks-event-replay] ---| Preparing main Dataframe |---')
        start_time = time.perf_counter()

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
        logger.info(f"[stacks-event-replay] finished in: {end_time - start_time:0.4f} seconds")

        return dataframe

    def split(self, df_main):
        logger.info(f"[stacks-event-replay] ---| Splitting main Dataframe |---")
        # Limit of block_height to be replayed.
        # After this limit, events will be processed by regular HTTP POSTS.
        max_block_height = df_main['block_height'].dropna().astype(int).max()
        block_height_limit = str(max_block_height - 256)

        # Split dataframe into two parts.
        # 1 - Events to be re-orged.
        # 2 - Remainder events to be replayed.
        row_index_limit = df_main.query('block_height == @block_height_limit').index.to_list()[0]
        df_to_reorg = df_main.iloc[:row_index_limit, :].copy(deep=False)
        df_remainder = df_main.iloc[row_index_limit:, :]

        logger.info(f"[stacks-event-replay] events will be reorged until block height: {block_height_limit}")

        return df_to_reorg, df_remainder

    def partition(self, df) -> None:
        """
        Create partitioned dataset from the dataframe.
        The partitions are based on the 'event' column.
        """

        logger.info('[stacks-event-replay] ---| Partitioning main Dataframe |---')
        start_time = time.perf_counter()

        df_reversed = df.sort_index(ascending=False)
        table = pa.Table.from_pandas(df_reversed)
        ds.write_dataset(
            table,
            base_dir='events',
            partitioning=['event'],
            format='parquet',
            existing_data_behavior='overwrite_or_ignore'
        )

        end_time = time.perf_counter()
        logger.info(f'[stacks-event-replay] finished in: {end_time - start_time:0.4f} seconds')

    def to_parquet(self, dataframe) -> None:
        logger.info('[stacks-event-replay] ---| Create parquet for remainder events |---')
        table = pa.Table.from_pandas(dataframe)
        ds.write_dataset(
            table,
            base_dir='events/remainder',
            format='parquet',
            existing_data_behavior='overwrite_or_ignore'
        )

    def get_new_block_dataset(self) -> pq.ParquetDataset:
        """
        Get new_block dataset
        """

        return ds.dataset('events/new_block/part-0.parquet', format="parquet")

    def get_attachments_dataset(self) -> pq.ParquetDataset:
        """
        Get new_block dataset
        """

        return ds.dataset('events/attachments/new/part-0.parquet', format="parquet")

    def get_new_burn_block_dataset(self) -> pq.ParquetDataset:
        """
        Get new_burn_block dataset
        """

        return ds.dataset('events/new_burn_block/part-0.parquet', format='parquet')
