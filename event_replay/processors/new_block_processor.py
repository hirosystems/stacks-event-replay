import json
import numpy as np
import event_replay.helper as files_helper

from event_replay.base_logger import logger

EVENT = "/new_block"

class NewBlockProcessor:
    """New Block Processor"""

    def __init__(self) -> None:
        pass

    def to_canonical(self, df):
        logger.info('NEW_BLOCK re-organization')

        canonical_indexes = []
        block_orphan_count = 0

        df = df.query('event == @EVENT').copy()
        df.sort_index(ascending=False, inplace=True)

        parent_index = json.loads(df.iloc[0]['payload'])['parent_index_block_hash']

        logger.info('---> running...')
        for i, frame in df.iloc[1:].iterrows():
            payload = json.loads(frame['payload'])
            index = payload['index_block_hash']

            if parent_index == index:
                canonical_indexes.append(index)
                if payload['block_height'] != 1: # not genesis
                    parent_index = payload['parent_index_block_hash']
            else:
                block_orphan_count += 1
                df.drop(i, inplace=True)
                continue

        if len(df) != 0:
            logger.info('---> saving canonical data')
            dfs = np.array_split(df, 10) # splitting dataframe since 'to_parquet' consumes a lot of RAM
            for data in dfs:
                files_helper.df_to_parquet("new_block/canonical", data)

        logger.info(f"---> canonical: {len(canonical_indexes) + 1}")
        logger.info('---> orphaned: %s', block_orphan_count)

        return df
