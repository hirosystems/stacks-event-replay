import os
import uuid

def df_to_parquet(path, df):
    if not os.path.exists(path):
        os.makedirs(f"events/{path}", exist_ok=True )

    df.to_parquet(f"events/{path}/{uuid.uuid4()}.parquet", engine='pyarrow')
