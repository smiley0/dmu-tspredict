#!/usr/bin/env python3

import pandas as pd
from os.path import join
import os
import requests

SRC_DIR = 'src'
OUT_DIR = 'csv'


def download():
    try:
        os.mkdir(SRC_DIR)
    except FileExistsError as e:
        pass
    
    links = [
        'https://archive.researchdata.leeds.ac.uk/272/1/DMU_Air_Temperatures.xlsx',
        'https://archive.researchdata.leeds.ac.uk/272/2/DMU_BHE_2010.xlsx',
        'https://archive.researchdata.leeds.ac.uk/272/3/DMU_BHE_2011.xlsx',
        'https://archive.researchdata.leeds.ac.uk/272/4/DMU_BHE_2012_2013.xlsx',
        'https://archive.researchdata.leeds.ac.uk/272/5/DMU_Compressor_Power.xlsx',
        'https://archive.researchdata.leeds.ac.uk/272/6/DMU_Cooling_Circulating_Pump.xlsx',
        'https://archive.researchdata.leeds.ac.uk/272/7/DMU_Cooling_Loop.xlsx',
        'https://archive.researchdata.leeds.ac.uk/272/8/DMU_Heating_Circulating_Pump.xlsx',
        'https://archive.researchdata.leeds.ac.uk/272/9/DMU_Heating_Loop.xlsx',
        'https://archive.researchdata.leeds.ac.uk/272/11/DMU_TRT.xlsx',
    ]
    
    for file in links:
        # Send a GET request to download the file
        r = requests.get(file, stream=True)
        
        fname = file.split('/')[-1]
        
        print(f'Downloading {file}:', end=' ')

        # Open a file with the same name as the file being downloaded
        with open(join(SRC_DIR, fname), 'wb') as f:
            # Iterate through the content of the file
            for chunk in r.iter_content(chunk_size=1024):
                # Write the content to the local file
                f.write(chunk)
                # Print the current download status
        print('done')

def load_bhe():
    files = [
        'DMU_BHE_2010.xlsx',
        'DMU_BHE_2011.xlsx',
        'DMU_BHE_2012_2013.xlsx',
    ]
    
    dfs = [pd.read_excel(join(SRC_DIR, f)) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    
    ts = pd.to_datetime(df.pop('DATE &TIME'))
    df.index = ts
    cols = ['T7(Ground Source Loop - In) (°C)', 'T8(Ground Source Loop - Out) (°C)', 'F1(Ground Loop -Flow ) (l/s)']
    
    return df[cols]


def load_sheets(file):
    df = pd.read_excel(join(SRC_DIR, file), index_col='DATE &TIME', parse_dates=True, sheet_name=None)
    dfs = [df[k] for k in sorted(df.keys())]
    dfc = pd.concat(dfs)
    
    return dfc


def convert_to_csv(log=True):
    l = (lambda m: print(f'Preparing {m}')) if log else (lambda m: None)
    
    l('DMU_BHE')
    bhe = load_bhe()
    bhe.to_csv(join(OUT_DIR, 'bhe.csv.gz'), compression='gzip')

    files = {
        'DMU_Cooling_Loop.xlsx': 'cooling_loop.csv.gz',
        'DMU_Cooling_Circulating_Pump.xlsx': 'cooling_pump.csv.gz',
        'DMU_Heating_Circulating_Pump.xlsx': 'heating_pump.csv.gz',
        'DMU_Heating_Loop.xlsx': 'heating_loop.csv.gz',
    }
    
    joined = bhe.copy()
    
    for src, dst in files.items():
        l(src)
        frame = load_sheets(src)
        frame.to_csv(join(OUT_DIR, dst), compression='gzip')
        joined = joined.join(frame, how='inner', rsuffix=dst.split('.')[0])

    joined.to_csv(join(OUT_DIR, 'all.csv.gz'), compression='gzip')

if __name__ == '__main__':
    #download()
    convert_to_csv()