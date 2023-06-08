from __future__ import annotations

import sys
import getopt
import logging
import time
import pandas as pd

logging.basicConfig(format='%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)

class ParquetGenerator:
    def __init__(self, tsv_path) -> None:
        self.name = 'Stacks Event Replay'
        self.tsv_path = tsv_path

    def run(self) -> None:
        self.to_parquet()

    def to_parquet(self) -> None:
        start_time = time.time()
        dataframe = pd.read_table(self.tsv_path)
        end_time = time.time()
        logger.info('[stacks-event-replay] reading %s TSV file finished in %s seconds', self.tsv_path, end_time - start_time)

        start_time = time.time()
        dataframe.to_parquet(f'{self.tsv_path}.parquet.gz', compression='gzip')
        end_time = time.time()
        logger.info('[stacks-event-replay] writing to Parquet file finished in %s seconds', end_time - start_time)

if __name__ == "__main__":
    logger.info('[stacks-event-replay] generating parquet file')

    tsv_file = ''

    opts, args = getopt.getopt(sys.argv[1:], '', ['tsv-file='])
    for opt, arg in opts:
        if opt in ('--tsv-file'):
            tsv_file = arg

    ParquetGenerator(tsv_file).run()
