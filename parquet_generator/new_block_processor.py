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
        logger.info('[stacks-event-replay] /new_block event processor started')
        self.dataset = dataset

    def to_canonical(self) -> None:
        start_time = time.time()

        # extract payload to a table on reverse order
        reverse_payload = self.dataset.sort_by([('id', 'descending')]).to_table(['payload'])

        # extract blockchain hashes from payload to a Table
        ibh  = []
        pibh = []
        for rp in reverse_payload:
            for data in rp:
                data_json = json.loads(data.as_py())
                pibh.append(data_json['parent_index_block_hash'])
                ibh.append(data_json['index_block_hash'])
        t = pa.table([pibh, ibh], names=['parent_index_block_hash', 'index_block_hash'])

        dataframe = t.to_pandas()
        parent = dataframe['parent_index_block_hash'].values[:1][0]
        forks = 0

        for i, frame in dataframe.iloc[1:].iterrows():
            idx = frame.index_block_hash
            if idx != parent:
                forks += 1
                parent = frame.parent_index_block_hash
                dataframe.drop(i, inplace=True)
                continue

            parent = frame.parent_index_block_hash

        end_time = time.time()
        logger.info('[stacks-event-replay] found %s forks in the blockchain data', forks)
        logger.info('[stacks-event-replay] /new_block event processor finished in %s seconds', end_time - start_time)
