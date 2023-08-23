import os
import uuid

def df_to_parquet_and_partition(filename, path, part, df):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True )

    df.to_parquet(f"{path}", partition_cols=[part], index=True)

def df_to_parquet(filename, path, df):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True )

    fullpath = os.path.join(path, filename)

    df.to_parquet(f"{fullpath}-{uuid.uuid4()}.parquet", index=True)
