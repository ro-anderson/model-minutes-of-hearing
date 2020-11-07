import os
import sys
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from external_libs.file_manager import FileManager

FM = FileManager()

global_df = pd.read_excel("data/external/references.xlsx")

def download_file(label='ata_audiencia', sample_size=100):
    mask = global_df['prediction'] == label
    sample_size = min(sample_size, mask.sum())
    s3_paths = global_df[mask].sample(sample_size)
    for i, p in enumerate(s3_paths['to']):
        key = "/".join(p.split('/')[3:])
        bucket = p.split('/')[2]
        FM.download_s3(key, bucket=bucket, local_prefix=f'data/external/sample/{label}/txt/{i:06d}_', use_stage=False)
    for i, p in enumerate(s3_paths['from']):
        key = "/".join(p.split('/')[3:])
        bucket = p.split('/')[2]
        FM.download_s3(key, bucket=bucket, local_prefix=f'data/external/sample/{label}/pdf/{i:06d}_', use_stage=False)                                                                      
