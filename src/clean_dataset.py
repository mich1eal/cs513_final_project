"""
Cleans Food_Inspections dataset

Michael Miller and Shane Sepac for University of Illinois CS 513
7/18/2023
"""

from os import path
import zipfile
import pandas as pd

PATH_RAW = path.join('..', 'data,', 'raw')
RAW_ZIP = 'Food_Inspections.zip'
RAW_CSV = 'Food_Inspections.csv'

# load raw data into dataframe

zipped = zipfile.ZipFile(path.join(PATH_RAW, RAW_ZIP))

raw_df = pd.read_csv(zipped.open(RAW_CSV))


