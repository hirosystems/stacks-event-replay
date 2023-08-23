import json
import event_replay.helper as files_helper

from event_replay.base_logger import logger

EVENT = "/attachments/new"

class AttachmentsProcessor:
    """New Burn Block Processor"""

    def __init__(self) -> None:
        pass

    def to_canonical(self, df, canonical_indexes: list):
        logger.info('ATTACHMENTS_NEW reorg started')

        attachment_canonical_count = 0
        attachment_orphan_count = 0

        canonical_indexes = canonical_indexes.index_block_hash.values.tolist()
        df = df.query('event == @EVENT').copy()
        for i, row in df.iterrows():
            payload = json.loads(row['payload'])
            attachment_indexes = [o['index_block_hash'] for o in payload]
            check =  all(idx in canonical_indexes for idx in attachment_indexes)
            if check:
                attachment_canonical_count += len(attachment_indexes)
            else:
                attachment_orphan_count += len(attachment_indexes)
                df.drop(i, inplace=True)
                continue

        if len(df) != 0:
            files_helper.df_to_parquet("attachments_new/canonical", df)

        logger.info(f"---> canonical: {attachment_canonical_count}")
        logger.info(f"---> orphaned: {attachment_orphan_count}")
