import json
import time
import logging

logging.basicConfig(format='%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

import pyarrow as pa
import pyarrow.dataset as ds

class AttachmentsProcessor:
    """New Burn Block Processor"""

    def __init__(self, dataset) -> None:
        self.dataset = dataset

    def to_canonical(self, canonical_indexes: list):
        logger.info('[stacks-event-replay] ATTACHMENTS_NEW reorg started')
        start_time = time.perf_counter()

        attachment_canonical_count = 0
        attachment_orphan_count = 0

        dataframe = self.dataset.to_table().to_pandas()
        logger.info('[stacks-event-replay] total: %s', len(dataframe.index))
        for i, row in dataframe.iterrows():
            attachment_indexes = [o['index_block_hash'] for o in json.loads(row['payload'])]
            check =  all(item in canonical_indexes for item in attachment_indexes)
            if check:
                attachment_canonical_count += len(attachment_indexes)
            else:
                attachment_orphan_count += len(attachment_indexes)
                dataframe.drop(i, inplace=True)
                continue

        self.dataset = pa.Table.from_pandas(dataframe)

        end_time = time.perf_counter()
        logger.info(f"[stacks-event-replay] canonical: {attachment_canonical_count}")
        logger.info(f"[stacks-event-replay] orphaned: {attachment_orphan_count}")
        logger.info(f"[stacks-event-replay] finished in: {end_time - start_time:0.4f} seconds")

        return self

    def save_dataset(self):
        ds.write_dataset(
            self.dataset,
            base_dir='events/attachments/new/canonical',
            format="parquet"
        )
