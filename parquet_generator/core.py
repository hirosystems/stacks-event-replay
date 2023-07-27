import logging
import json
import time
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

logging.basicConfig(format='%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

class CoreEventsProcessor:
    def __init__(self, tsv_path) -> None:
        logger.info('[stacks-event-replay] reorganization of Stacks blockchain events into Parquet files')
        logger.info('[stacks-event-replay] processing a TSV file: %s', tsv_path)
        self.tsv_path = tsv_path

    def main_dataframe(self) -> pd.DataFrame:
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

        return dataframe

    def split(self, df_main):
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

        return df_to_reorg, df_remainder

    def partition(self, dataframe) -> None:
        """
        Create partitioned dataset from the dataframe.
        The partitions are based on the 'event' column.
        """

        table = pa.Table.from_pandas(dataframe)
        ds.write_dataset(
            table,
            base_dir='events',
            partitioning=['event'],
            format='parquet',
            existing_data_behavior='overwrite_or_ignore'
        )

    def to_parquet(self, dataframe) -> None:
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

        start_time = time.time()
        dataset = ds.dataset('events/new_block/part-0.parquet', format="parquet")
        end_time = time.time()
        logger.info('[stacks-event-replay] reading NEW_BLOCK dataset finished in %s seconds', end_time - start_time)

        return dataset

    def get_new_burn_block_dataset(self) -> pq.ParquetDataset:
        """
        Get new_burn_block dataset
        """

        start_time = time.time()
        new_burn_block_dataset = ds.dataset('events/new_burn_block/part-0.parquet', format='parquet')
        end_time = time.time()
        logger.info('[stacks-event-replay] reading NEW_BURN_BLOCK dataset finished in %s seconds', end_time - start_time)

        return new_burn_block_dataset