"""
Cleans Food_Inspections dataset

Michael Miller and Shane Sepac for University of Illinois CS 513
7/18/2023
"""

from os import path
import zipfile
import pandas as pd
import re
from assertion_chain import AssertionChain


# constant paths and files 
PATH_RAW = path.join('..', 'data', 'raw')
RAW_ZIP = 'Food_Inspections.zip'
RAW_CSV = 'Food_Inspections.csv'

PATH_OUT = path.join('..', 'data', 'processed')
PATH_LOG = path.join('..', 'data', 'log')

VALID_RISKS = ['Risk 1 (High)', 'Risk 2 (Medium)', 'Risk 3 (Low)']
VALID_RESULTS = ['Pass', 'Fail', 'Pass w/ Conditions']


############### helper functions ################
def has_outside_whitespace(string):
    """true if first or last char is whitespace"""
    return bool(re.search(r'\s$', string)) or bool(re.search(r'^\s', string))

def trim_column(df, column):
    """trim whitespace from name column"""
    df.loc[:, column] = df.loc[:, column].apply(lambda name: name.strip())
    return df

def make_column_upper(df, column):
    """make a df column uppercase"""
    df.loc[:, column] = df.loc[:, column].str.upper()
    return df


############### load data ################
# load raw data into dataframe
print('Loading data')
zipped = zipfile.ZipFile(path.join(PATH_RAW, RAW_ZIP))
raw_df = pd.read_csv(zipped.open(RAW_CSV))


############### setup AssertionChain ################
chain = AssertionChain(out_path=PATH_OUT, explore_path=PATH_LOG)

# checks on Inspection ID
#inspection ID should be unique
id_not_null = lambda df: ~df['Inspection ID'].isnull() & ~df['Inspection ID'].isna()
chain.add(name='Inspection ID not null', clean_assert=id_not_null)
chain.add(name='Inspection ID is int', clean_assert=lambda df: df['Inspection ID'].apply(lambda val: isinstance(val, int)))
chain.add(name='Inspection ID is unique',  
          clean_assert=lambda df: ~df.duplicated('Inspection ID', keep='first'))

# checks on license 
#license should exist, be >0
license_not_null = lambda df: ~df['License #'].isnull() & ~df['License #'].isna()
chain.add(name='License not null', clean_assert=license_not_null)
chain.add(name='License is int', clean_assert=lambda df: df['License #'].apply(float.is_integer))
chain.add(name='License > 0', clean_assert=lambda df: df['License #'] > 0)

# checks on name
chain.add(name='Name not null', clean_assert=lambda df: ~df['DBA Name'].isnull())
chain.add(name='Name is str', 
          clean_assert=lambda df: df['DBA Name'].apply(lambda val: isinstance(val, str)))

#name can't have leading/trailing whitespace
no_whitespace = lambda df: ~df['DBA Name'].apply(has_outside_whitespace)

chain.add(name='Name has no outside whitespace', 
          clean_assert=no_whitespace,
          operation='apply',
          resolve=lambda df: trim_column(df, 'DBA Name'))

#name can't be blank
chain.add(name='Name not blank', clean_assert=lambda df: df['DBA Name'].str.len() > 0)


#name should be all caps
chain.add(name='Name is all caps', 
          clean_assert=lambda df: ~df['DBA Name'].str.islower(),
          operation='apply',
          resolve=lambda df: make_column_upper(df, 'DBA Name'))

# checks on risk
#drop blanks or 'All'
chain.add(name='Risk is valid', clean_assert=lambda df: df['Risk'].isin(VALID_RISKS))

# checks on results
chain.add(name='Results is valid', clean_assert=lambda df: df['Results'].isin(VALID_RESULTS))


# checks on date
is_valid_date = lambda df: pd.to_datetime(df['Inspection Date'], errors='coerce').notnull()
chain.add(name='Date not null', clean_assert=lambda df: ~df['Inspection Date'].isnull())
chain.add(name='Date is valid', clean_assert=is_valid_date)


############### run AssertionChain ################
# explore assertion chain to understand impacts on dataset
chain.explore_chain(raw_df)

# run assertion chain
cleaned_df = chain.apply_chain(raw_df)

# validate assertion chain
chain.validate_chain(cleaned_df)