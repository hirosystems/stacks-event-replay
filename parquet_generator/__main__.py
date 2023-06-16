import sys
import getopt

from .core import ParquetGenerator
from parquet_generator.processors.new_block_processor import NewBlockProcessor
from parquet_generator.processors.new_burn_block_processor import NewBurnBlockProcessor

if __name__ == "__main__":
    tsv_file = ''

    opts, args = getopt.getopt(sys.argv[1:], '', ['tsv-file='])
    for opt, arg in opts:
        if opt in ('--tsv-file'):
            tsv_file = arg

    gen = ParquetGenerator(tsv_file)

    # -- dataframe partitioning
    dataframe = gen.dataframe()
    gen.partition(dataframe)

    # -- process new_block dataset
    new_block_dataset = gen.get_new_block_dataset()
    NewBlockProcessor(new_block_dataset).to_canonical().save_dataset()

    # -- new_burn_blocks
    new_burn_blocks_dataset = gen.get_new_burn_block_dataset()
    NewBurnBlockProcessor(new_burn_blocks_dataset).to_canonical().save_dataset()
