import os
import requests
import pandas as pd
import gzip
import tarfile
import scipy.io
from io import BytesIO
import shutil

datasets = {
    'muv': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/muv.csv.gz',
    'HIV': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/HIV.csv',
    'pdbbind': 'http://deepchem.io.s3-website-us-west-1.amazonaws.com/datasets/pdbbind_v2015.tar.gz',
    'bace': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/bace.csv',
    'BBBP': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/BBBP.csv',
    'tox21': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/tox21.csv.gz',
    'toxcast': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/toxcast_data.csv.gz',
    'sider': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/sider.csv.gz',
    'clintox': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/clintox.csv.gz',
    'qm8': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/qm8.csv',
    'qm9': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/qm9.csv',
    'delaney': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/delaney-processed.csv',
    'SAMPL': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/SAMPL.csv',
    'Lipophilicity': 'https://deepchemdata.s3-us-west-1.amazonaws.com/datasets/Lipophilicity.csv'
}

def download_and_extract_pdbbind(url, extract_to_folder):
    response = requests.get(url)
    with tarfile.open(mode="r:gz", fileobj=BytesIO(response.content)) as tar:
        tar.extractall(extract_to_folder)

def move_pdbbind_to_brick(source_folder, target_folder):
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
    for subdir in os.listdir(source_folder):
        subdir_path = os.path.join(source_folder, subdir)
        if os.path.isdir(subdir_path):
            shutil.move(subdir_path, target_folder)

def download_and_convert(url, name):
    try:
        if name == 'pdbbind':
            temp_extraction_folder = 'temp_pdbbind_extraction'
            download_and_extract_pdbbind(url, temp_extraction_folder)
            move_pdbbind_to_brick(temp_extraction_folder, 'brick')
            shutil.rmtree(temp_extraction_folder)  # Clean up
        else:
            response = requests.get(url)
            if url.endswith('.gz'):
                with gzip.open(BytesIO(response.content), 'rt') as f:
                    df = pd.read_csv(f, on_bad_lines='skip')
            elif url.endswith('.mat'):
                mat_data = scipy.io.loadmat(BytesIO(response.content))
                df = pd.DataFrame(mat_data)  # Adjust as needed for .mat files
            else:
                df = pd.read_csv(BytesIO(response.content), on_bad_lines='skip')
            parquet_file_path = os.path.join('brick', f'{name}.parquet')
            df.to_parquet(parquet_file_path)
    except Exception as e:
        print(f"Error processing {name}: {e}")

if not os.path.exists('brick'):
    os.makedirs('brick')

for name, url in datasets.items():
    print(f"Processing {name} dataset...")
    download_and_convert(url, name)
