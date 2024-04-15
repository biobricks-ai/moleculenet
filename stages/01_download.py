import os, pathlib, requests, hashlib, tqdm

file_hash = hashlib.md5(open('download/data-links.txt', 'rb').read()).hexdigest()
msg = "ATTENTION: Primary data source updated. Script currently hardcoded for original source. Modify script to accommodate new source."
assert file_hash == '605b8cc26244681afec7a73e3f29aecd', msg

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

download = pathlib.Path('download') / "01_download"
download.mkdir(exist_ok=True)

for url in datasets.values():
    print(f"Processing {url} dataset...")
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Ensure the HTTP request was successful    
    pb = tqdm.tqdm(total=int(response.headers.get('content-length', 0)), unit='iB', unit_scale=True)
    target_path = download / os.path.basename(url)
    with open(target_path, 'wb') as f, pb as bar:
        for data in response.iter_content(chunk_size=1024):
            size = f.write(data)
            _ = bar.update(size)