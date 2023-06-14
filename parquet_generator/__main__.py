import sys
import getopt

sys.path.insert(0, '/processors/new_block_processor/')

"""Parquet Generator entrypoint"""
from .core import ParquetGenerator
from .new_block_processor import NewBlockProcessor

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
    # new_burn_blocks = gen.get_new_burn_block_dataset()
    # print(new_burn_blocks.read().to_pandas())
