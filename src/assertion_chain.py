"""
Michael Miller and Shane Sepac for University of Illinois CS 513
7/18/2023
"""
import pandas as pd
from os import path, listdir, remove, makedirs

class AssertionChain:
    """
    AssertionChain applies DRY principle to data cleaning. A chain of assertions
    are defined which must be true for a dataset to be considered clean.
    
    There are three options for running a query chain: 
        explore - generates console output and log files capture what data will be altered
            when chain is run 
        run - drops any data which does not meet assertions
        validate - determines whether a validate meets all asserts 
    """
    
    def __init__(self, out_path: str, explore_path: str):
        self.assertion_chain = []
        
        # set up directories. Clear them, create if not exists         
        self.out_path = out_path
        self.explore_path = explore_path
        
        for directory in (out_path, explore_path):    
            if path.exists(directory):
            # Clear the directory
                for filename in listdir(directory):
                    file_path = path.join(directory, filename)
                    remove(file_path)
                print(f"Directory '{directory}' cleared.")
            else:
                # Create the directory
                makedirs(directory)
                print(f"Directory '{directory}' created.")
     
    def _get_failures(self, df: pd.DataFrame, assertion: dict) -> pd.DataFrame:
        """
        Apply an assertion, return rows that fail the assertion
        """
        valid_idx = assertion['clean_assert'](df)
        fail_rows = df[~valid_idx]
        return fail_rows
         
    def _resolve_failures(self, df: pd.DataFrame, assertion: dict) -> pd.DataFrame:
        """
        Applies this assertions resolution function
        """
        valid_idx = assertion['clean_assert'](df)
        
        # for drop we simply select valid rows
        if assertion['operation'] == 'drop':
            df = df[valid_idx]
        
        # for apply, we apply resolve function to invalid rows 
        elif assertion['operation'] == 'apply':
            df[~valid_idx] = assertion['resolve'](df[~valid_idx])
            
        return df
     
    def _validate_assertion(self, df: pd.DataFrame, assertion: dict):
         """
         Throws assertion error if assertion is not met 
         """
         fail_count = len(self._get_failures(df, assertion).index)
         assert fail_count == 0, f'Assertion {assertion["name"]} failed {fail_count} times'
       
    def add(self, name: str, clean_assert: callable, operation:str='drop', resolve:callable=None):
        """
        The new assertion will be applied as part of this AssertionChain. Note that order matters!
        """
        assert operation in ('drop', 'apply'), "'Operation must be either 'drop' or 'apply'"

        self.assertion_chain.append({'name': name,
                                     'operation': operation,
                                     'clean_assert': clean_assert,
                                     'resolve': resolve})
   
    def explore_chain(self, df: pd.DataFrame) -> pd.DataFrame():
        """
        Applies each assertion in the chain, outputting failed rows for each assertion
        to explore_path. 
        Saves final claned data in explore_path and returns it
        """
        print('EXPORING DATASET')
        
        for i, assertion in enumerate(self.assertion_chain):
            print(f'Assertion {i}: {assertion["name"]}:')
            
            # apply function to dataframe
            fail_rows = self._get_failures(df, assertion)
            
            # calculate metrics about this assertion
            fail_count = len(fail_rows.index)
            fail_percent = 100 * fail_count / len(df.index)
            fail_file = f'assertion{i}_failed_rows.csv'
            
            # write output
            print(f'\t{fail_count} rows fail ({fail_percent:.2f}%)')
            print(f'\tFailed rows saved as {fail_file}')
            fail_rows.to_csv(path.join(self.explore_path, fail_file))
            
            # resolve failures so we can proceed to next assertion
            print('\tApplying resolution function')
            df = self._resolve_failures(df, assertion)
            print(f'\tRemaining rows: {len(df.index)}')
            
        
        # output assertion log for reference 
        assertion_log = pd.DataFrame(self.assertion_chain)
        assertion_log = assertion_log['name']
        assertion_log.rename_axis('Assertion', inplace=True)
        assertion_log.to_csv(path.join(self.explore_path, 'assertions.csv'))
        
        # output the cleaned data
        df.to_csv(path.join(self.out_path, 'exploration_results.csv'))
        return df
    
    def apply_chain(self, df: pd.DataFrame) -> pd.DataFrame():
        """
        Applies assertion chain, outputs cleaned data and also returns it 
        """
        print('APPLYING ASSERTION CHAIN')
        
        assertion_count = len(self.assertion_chain)
        for i, assertion in enumerate(self.assertion_chain):
            print(f'\tApplying assertion {i} of {assertion_count}')
            df = self._resolve_failures(df, assertion)
            
        # output the cleaned data
        df.to_csv(path.join(self.out_path, 'cleaned_data.csv'))
        return df

    def validate_chain(self, df: pd.DataFrame) -> pd.DataFrame():
        """
        Applies assertion chain, outputs cleaned data and also returns it 
        """
        print('VALIDATING DATASET')
        for i, assertion in enumerate(self.assertion_chain):
            print(f'\tAsserting: {assertion["name"]}')
            self._validate_assertion(df, assertion)
