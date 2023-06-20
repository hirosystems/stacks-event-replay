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
        logger.info('[stacks-event-replay] NEW_BLOCK event processor started')

        start_time = time.time()
        block_canonical_count = 0
        block_orphan_count = 0

        dataframe = self.dataset.to_table().to_pandas()
        dataframe.sort_index(ascending=False, inplace=True)

        parent_index = json.loads(dataframe.iloc[0]['payload'])['parent_index_block_hash']

        logger.info('[stacks-event-replay] /new_block: %s', len(dataframe.index))
        for i, frame in dataframe.iloc[1:].iterrows():
            payload = json.loads(frame['payload'])
            index = payload['index_block_hash']

            if parent_index == index:
                block_canonical_count += 1
                # TODO memo microblock
                if payload['block_height'] != 1: # not genesis
                    parent_index = payload['parent_index_block_hash']
            else:
                block_orphan_count += 1
                dataframe.drop(i, inplace=True)
                continue

        self.dataset = pa.Table.from_pandas(dataframe)

        end_time = time.time()
        logger.info('[stacks-event-replay] canonical.: %s', block_canonical_count)
        logger.info('[stacks-event-replay] orphaned..: %s', block_orphan_count)
        logger.info('[stacks-event-replay] NEW_BLOCK event processor finished in %s seconds', end_time - start_time)
        return self

    def save_dataset(self):
        ds.write_dataset(
            self.dataset,
            base_dir='events/new_block/canonical',
            format="parquet"
        )
