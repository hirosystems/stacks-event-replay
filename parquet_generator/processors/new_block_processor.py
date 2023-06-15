import json
import time
import logging

logging.basicConfig(format='%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

import pyarrow as pa
import pyarrow.dataset as ds

class NewBlockProcessor:
    """New Block Processor"""

    def __init__(self, dataset) -> None:
        self.dataset = dataset

    def to_canonical(self):
        logger.info('[stacks-event-replay] /new_block event processor started')

        start_time = time.time()
        forks = 0;

        # extract payload to a table on reverse order
        reverse_payload = self.dataset.sort_by([('id', 'descending')]).to_table(['id', 'payload'])
        parent = json.loads(reverse_payload[1][0].as_py())['parent_index_block_hash']

        dataframe = reverse_payload.to_pandas()
        for i, frame in dataframe.iloc[1:].iterrows():
            idx = json.loads(frame['payload'])['index_block_hash']
            if idx != parent:
                forks += 1
                parent = json.loads(frame['payload'])['parent_index_block_hash']
                dataframe.drop(i, inplace=True)
                continue

            parent = json.loads(frame['payload'])['parent_index_block_hash']

        self.dataset = pa.Table.from_pandas(dataframe)

        logger.info('[stacks-event-replay] %s forks detected in the blockchain data', forks)
        end_time = time.time()
        logger.info('[stacks-event-replay] /new_block event processor finished in %s seconds', end_time - start_time)
        return self

    def save_dataset(self):
        ds.write_dataset(
            self.dataset,
            base_dir='events/new_block/canonical',
            format="parquet"
        )
