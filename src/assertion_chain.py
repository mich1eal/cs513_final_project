"""
Michael Miller and Shane Sepac for University of Illinois CS 513
7/18/2023
"""
import Pandas as pd 


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
    
    def __init__(self):
        self.assertion_chain = []
    
    
    def add_assertion(self, name: str, clean_assert: callable, resolve: callable):
        """
        Assert will be applied as part of this AssertionChain. Note that order matters!
        

        Parameters
        ----------
        clean_assert : callable
            
            
        resolve : callable
            called 

        Returns
        -------
        None.

        """
        self.assertion_chain.append((name, clean_assert, resolve))
        
    
    def explore(self, df: pd.Dataframe):
        
        for i, assertion in enumerate(self.assertion_chain):
            name, clean_assert, resolve = assertion 
            print(f'Assertion {i}: {name}:\n')
            
            # apply function to dataframe
            valid_idx = clean_assert(df)
            fail_rows = df[~valid_idx]
            
            # calculate metrics about this assertion
            fail_count = len(fail_rows.index)
            fail_percent = 100 * fail_count / len(df.index)
            fail_file = f'assertion{i}_failed_rows.csv'
            
            print(f'/t{fail_count} rows fail ({fail_percent}%). Failed rows saved as {fail_file}')
            fail_rows.to_csv(fail_file)
           
        
        
            
        
        
        
        
        
        
        
        
    
        
    
        
        
        
        
        
        
        
        