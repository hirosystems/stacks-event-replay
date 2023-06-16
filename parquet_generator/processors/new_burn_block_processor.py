import json
import time
import logging

logging.basicConfig(format='%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

import pyarrow as pa
import pyarrow.dataset as ds

class NewBurnBlockProcessor:
    """New Burn Block Processor"""

    def __init__(self, dataset) -> None:
        self.dataset = dataset

    def to_canonical(self):
        logger.info('[stacks-event-replay] /new_burn_block event processor started')

        start_time = time.time()
        burn_block_canonical_count = 1
        burn_block_orphan_count = 0

        # extract payload to a table on reverse order
        reverse_payload = self.dataset.sort_by([('id', 'descending')]).to_table(['id', 'payload'])
        burn_block_hash = json.loads(reverse_payload[1][0].as_py())['burn_block_hash']
        burn_block_height = json.loads(reverse_payload[1][0].as_py())['burn_block_height']

        dataframe = reverse_payload.to_pandas()
        logger.info('[stacks-event-replay] /new_burn_block: %s', len(dataframe.index))
        for i, frame in dataframe.iloc[1:].iterrows():
            current_burn_block_hash = json.loads(frame['payload'])['burn_block_hash']
            current_burn_block_height = json.loads(frame['payload'])['burn_block_height']

            if current_burn_block_height >= burn_block_height:
                burn_block_orphan_count += 1
                dataframe.drop(i, inplace=True)
                continue
            elif current_burn_block_hash == burn_block_hash:
                burn_block_orphan_count += 1
                dataframe.drop(i, inplace=True)
                continue

            burn_block_hash = json.loads(frame['payload'])['burn_block_hash']
            burn_block_height = json.loads(frame['payload'])['burn_block_height']
            burn_block_canonical_count += 1

        self.dataset = pa.Table.from_pandas(dataframe)

        end_time = time.time()
        logger.info('[stacks-event-replay] canonical......: %s', burn_block_canonical_count)
        logger.info('[stacks-event-replay] orphaned.......: %s', burn_block_orphan_count)
        logger.info('[stacks-event-replay] /new_burn_block event processor finished in %s seconds', end_time - start_time)
        return self

    def save_dataset(self):
        ds.write_dataset(
            self.dataset,
            base_dir='events/new_burn_block/canonical',
            format="parquet"
        )
