import logging
import array
import json
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

import csv

logging.basicConfig(format='%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ParquetGenerator:
    """
    Generate a reorged Parquet files into partitions
    """

    def __init__(self, tsv_path) -> None:
        logger.info('[stacks-event-replay] reorganization of Stacks blockchain events into Parquet files')
        self.tsv_path = tsv_path

    def dataframe(self) -> pd.DataFrame:
        """
        Read a TSV file and generate a dataframe
        """

        start_time = time.time()
        dataframe = pd.read_table(
            self.tsv_path,
            compression='gzip',
            header=None,
            names=['id', 'timestamp', 'event', 'payload']
        )

        blockHeight = [];
        index_block_hashes = []
        parent_index_block_hashes = []
        event_hashes = []

        for i, frame in dataframe.iterrows():
            payload = json.loads(frame['payload'])
            if frame['event'] == '/new_block':
                blockHeight.append(str(payload['block_height']));
                index_block_hashes.append(payload['index_block_hash'])
                parent_index_block_hashes.append(payload['parent_index_block_hash'])
                event_hashes.append('/new_block')
            elif frame['event'] == '/new_microblocks':
                blockHeight.append('');
                index_block_hashes.append('')
                parent_index_block_hashes.append(payload['parent_index_block_hash'])
                event_hashes.append('/new_microblocks')
            elif frame['event'] == '/new_burn_block':
                blockHeight.append('');
                index_block_hashes.append('')
                parent_index_block_hashes.append('')
                event_hashes.append('/new_burn_block')
            elif frame['event'] == '/new_mempool_tx':
                blockHeight.append('');
                index_block_hashes.append('')
                parent_index_block_hashes.append('')
                event_hashes.append('/new_mempool_tx')
            elif frame['event'] == '/drop_mempool_tx':
                blockHeight.append('');
                index_block_hashes.append('')
                parent_index_block_hashes.append('')
                event_hashes.append('/drop_mempool_tx')
            else:
              print(payload)
              blockHeight.append('');
              index_block_hashes.append('')
              parent_index_block_hashes.append('')
              event_hashes.append('/attachments/new')

        dataframe.insert(4, 'block_height', blockHeight)
        dataframe.insert(5, 'index_block_hash', index_block_hashes)
        dataframe.insert(6, 'parent_index_block_hash', parent_index_block_hashes)
        dataframe.insert(7, 'method', event_hashes)

        end_time = time.time()
        logger.info('[stacks-event-replay] partitioning %s TSV file finished in %s seconds', self.tsv_path, end_time - start_time)

        return dataframe

    def partition(self, dataframe) -> None:
        """
        Create partitioned dataset from the pandas dataframe
        """

        table = pa.Table.from_pandas(dataframe)
        ds.write_dataset(
            table,
            base_dir='events',
            partitioning=['event'],
            format="parquet"
        )

    def get_new_block_dataset(self) -> pq.ParquetDataset:
        """
        Read new_block dataset
        """

        start_time = time.time()
        new_block_dataset = ds.dataset('events/new_block/part-0.parquet', format="parquet")
        end_time = time.time()
        logger.info('[stacks-event-replay] reading NEW_BLOCK dataset finished in %s seconds', end_time - start_time)

        return new_block_dataset

    def get_new_burn_block_dataset(self) -> pq.ParquetDataset:
        """
        Read new_burn_block dataset
        """

        start_time = time.time()
        new_burn_block_dataset = ds.dataset('events/new_burn_block/part-0.parquet', format='parquet')
        end_time = time.time()
        logger.info('[stacks-event-replay] reading NEW_BURN_BLOCK dataset finished in %s seconds', end_time - start_time)

        return new_burn_block_dataset