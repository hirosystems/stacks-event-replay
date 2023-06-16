import logging
import time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

logging.basicConfig(format='%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ParquetGenerator:
    """
    Generate a reorged Parquet files into partitions
    """

    def __init__(self, tsv_path) -> None:
        logger.info('[stacks-event-replay] Parquet partitions generator')
        self.name = 'Stacks Event Replay'
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
        end_time = time.time()
        logger.info('[stacks-event-replay] reading %s TSV file finished in %s seconds', self.tsv_path, end_time - start_time)

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
        logger.info('[stacks-event-replay] reading new_block dataset finished in %s seconds', end_time - start_time)

        return new_block_dataset

    def get_new_burn_block_dataset(self) -> pq.ParquetDataset:
        """
        Read new_burn_block dataset
        """

        start_time = time.time()
        new_burn_block_dataset = ds.dataset('events/new_burn_block/', format='parquet')
        end_time = time.time()
        logger.info('[stacks-event-replay] reading new_burn_block dataset finished in %s seconds', end_time - start_time)

        return new_burn_block_dataset
