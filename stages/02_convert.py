import shutil, pathlib, pandas as pd, tqdm, re

downdir = pathlib.Path('download') / "01_download"
brikdir = pathlib.Path('brick')
brikdir.mkdir(exist_ok=True)

processed_files = []

# find all csvs or csvs.gz in the download directory
for csv in tqdm.tqdm(list(downdir.glob('*.csv')) + list(downdir.glob('*.csv.gz'))):
    processed_files = processed_files + [csv]
    outpath = brikdir / re.sub(r'\.csv(\.gz)?$', '.parquet', csv.name)
    pd.read_csv(csv).to_parquet(outpath)

# copy tar.gz files to the brick directory
for tar_gz in list(downdir.glob('*.tar.gz')):
    processed_files = processed_files + [tar_gz]
    shutil.copy(tar_gz, brikdir)

# check that all files were converted by using setdiff
diff = set(list(downdir.glob('*'))) - set(processed_files)
assert len(diff) == 0, f"Files not processed: {diff}"
