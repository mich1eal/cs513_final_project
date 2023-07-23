"""
Cleans Food_Inspections dataset

Michael Miller and Shane Sepac for University of Illinois CS 513
7/18/2023
"""

from os import path
import zipfile
import pandas as pd
from assertion_chain import AssertionChain


# constant paths and files 
PATH_RAW = path.join('..', 'data', 'raw')
RAW_ZIP = 'Food_Inspections.zip'
RAW_CSV = 'Food_Inspections.csv'

PATH_OUT = path.join('..', 'data', 'processed')
PATH_LOG = path.join('..', 'data', 'log')

# load raw data into dataframe
zipped = zipfile.ZipFile(path.join(PATH_RAW, RAW_ZIP))
raw_df = pd.read_csv(zipped.open(RAW_CSV))

# set up AssertionCHain for cleaning 
chain = AssertionChain(out_path=PATH_OUT, explore_path=PATH_LOG)

# add assertions that we want to check, and functions to resolve (drop by default)
license_not_null = lambda df: ~df['License #'].isnull() & ~df['License #'].isna()
chain.add(name='License not null', clean_assert=license_not_null)

chain.add(name='License > 0', clean_assert=lambda df: df['License #'] > 0)

# explore assertion chain to understand impacts on dataset
chain.explore_chain(raw_df)

# run assertion chain
cleaned_df = chain.apply_chain(raw_df)

# validate assertion chain
chain.validate_chain(cleaned_df)

