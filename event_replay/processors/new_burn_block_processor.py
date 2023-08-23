import json
import event_replay.helper as files_helper

from event_replay.base_logger import logger

EVENT = "/new_burn_block"

class NewBurnBlockProcessor:
    """New Burn Block Processor"""

    def __init__(self) -> None:
        pass

    def to_canonical(self, df):
        logger.info('NEW_BURN_BLOCK re-organization')

        burn_block_canonical_count = 1
        burn_block_orphan_count = 0

        logger.info('---> running...')
        df = df.query('event == @EVENT').copy()

        df = df.sort_values(by=['id'], ascending=False)
        df = df[['id', 'payload', 'method']]
        burn_block_hash = json.loads(df.iloc[0].payload)['burn_block_hash']
        burn_block_height = json.loads(df.iloc[0].payload)['burn_block_height']

        for i, frame in df.iloc[1:].iterrows():
            current_burn_block_hash = json.loads(frame['payload'])['burn_block_hash']
            current_burn_block_height = json.loads(frame['payload'])['burn_block_height']

            if current_burn_block_height >= burn_block_height:
                burn_block_orphan_count += 1
                df.drop(i, inplace=True)
                continue
            elif current_burn_block_hash == burn_block_hash:
                burn_block_orphan_count += 1
                df.drop(i, inplace=True)
                continue

            burn_block_hash = json.loads(frame['payload'])['burn_block_hash']
            burn_block_height = json.loads(frame['payload'])['burn_block_height']
            burn_block_canonical_count += 1

        if len(df) != 0:
            files_helper.df_to_parquet("new_burn_block/canonical", df)

        logger.info('---> canonical: %s', burn_block_canonical_count)
        logger.info('---> orphaned: %s', burn_block_orphan_count)
