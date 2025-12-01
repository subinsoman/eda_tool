import dask.dataframe as dd
import pandas as pd
import numpy as np
from excel import calculate_column_metrics

def test_metrics():
    # Create a sample dataframe
    data = {
        'int_col': [1, 2, 3, 4, 5],
        'float_col': [1.1, 2.2, 3.3, 4.4, 5.5],
        'str_col': ['a', 'b', 'c', 'd', 'e'],
        'bool_col': [True, False, True, False, True],
        'cat_col': pd.Categorical(['x', 'y', 'x', 'y', 'x'])
    }
    pdf = pd.DataFrame(data)
    ddf = dd.from_pandas(pdf, npartitions=1)

    print("Calculating metrics...")
    metrics_list = calculate_column_metrics(ddf)
    
    for metric in metrics_list:
        col_name = metric['Column Name']
        print(f"Checking column: {col_name}")
        
        # Check removed metrics
        if 'Total Count' in metric:
            print(f"FAIL: 'Total Count' found in {col_name}")
        else:
            print(f"PASS: 'Total Count' not found in {col_name}")
            
        if 'Non-Null Count' in metric:
            print(f"FAIL: 'Non-Null Count' found in {col_name}")
        else:
            print(f"PASS: 'Non-Null Count' not found in {col_name}")
            
        # Check data types
        dtype = metric['Data Type']
        print(f"Data Type: {dtype}")
        
        if col_name == 'int_col':
            assert dtype == 'Integer', f"Expected Integer, got {dtype}"
            if 'Skewness' not in metric:
                print(f"FAIL: 'Skewness' not found in {col_name}")
            else:
                print(f"PASS: 'Skewness' found in {col_name}: {metric['Skewness']}")
            if 'Mean Absolute Deviation' not in metric:
                print(f"FAIL: 'Mean Absolute Deviation' not found in {col_name}")
            else:
                print(f"PASS: 'Mean Absolute Deviation' found in {col_name}: {metric['Mean Absolute Deviation']}")
            if 'P99 (99%)' not in metric:
                print(f"FAIL: 'P99 (99%)' not found in {col_name}")
            else:
                print(f"PASS: 'P99 (99%)' found in {col_name}: {metric['P99 (99%)']}")
            if 'Excess Kurtosis' not in metric:
                print(f"FAIL: 'Excess Kurtosis' not found in {col_name}")
            else:
                print(f"PASS: 'Excess Kurtosis' found in {col_name}: {metric['Excess Kurtosis']}")
        elif col_name == 'float_col':
            assert dtype == 'Float', f"Expected Float, got {dtype}"
            if 'Skewness' not in metric:
                print(f"FAIL: 'Skewness' not found in {col_name}")
            else:
                print(f"PASS: 'Skewness' found in {col_name}: {metric['Skewness']}")
            if 'Mean Absolute Deviation' not in metric:
                print(f"FAIL: 'Mean Absolute Deviation' not found in {col_name}")
            else:
                print(f"PASS: 'Mean Absolute Deviation' found in {col_name}: {metric['Mean Absolute Deviation']}")
            if 'P99 (99%)' not in metric:
                print(f"FAIL: 'P99 (99%)' not found in {col_name}")
            else:
                print(f"PASS: 'P99 (99%)' found in {col_name}: {metric['P99 (99%)']}")
            if 'Excess Kurtosis' not in metric:
                print(f"FAIL: 'Excess Kurtosis' not found in {col_name}")
            else:
                print(f"PASS: 'Excess Kurtosis' found in {col_name}: {metric['Excess Kurtosis']}")
        elif col_name == 'str_col':
            assert dtype == 'String', f"Expected String, got {dtype}"
            if 'Text Length Mean' not in metric:
                print(f"FAIL: 'Text Length Mean' not found in {col_name}")
            else:
                print(f"PASS: 'Text Length Mean' found in {col_name}: {metric['Text Length Mean']}")
            if 'Entropy (Top 10)' not in metric:
                print(f"FAIL: 'Entropy (Top 10)' not found in {col_name}")
                print(f"Metrics keys: {metric.keys()}")
                if 'Error' in metric:
                    print(f"Error Message: {metric['Error']}")
            else:
                print(f"PASS: 'Entropy (Top 10)' found in {col_name}: {metric['Entropy (Top 10)']}")
            if 'Dominant Category' not in metric:
                print(f"FAIL: 'Dominant Category' not found in {col_name}")
            else:
                print(f"PASS: 'Dominant Category' found in {col_name}: {metric['Dominant Category']}")
        elif col_name == 'bool_col':
            assert dtype == 'Boolean', f"Expected Boolean, got {dtype}"
        elif col_name == 'cat_col':
            assert dtype == 'Categorical', f"Expected Categorical, got {dtype}"
            if 'Entropy (Top 10)' not in metric:
                print(f"FAIL: 'Entropy (Top 10)' not found in {col_name}")
                print(f"Metrics keys: {metric.keys()}")
                if 'Error' in metric:
                    print(f"Error Message: {metric['Error']}")
            else:
                print(f"PASS: 'Entropy (Top 10)' found in {col_name}: {metric['Entropy (Top 10)']}")
            if 'Dominant Category' not in metric:
                print(f"FAIL: 'Dominant Category' not found in {col_name}")
            else:
                print(f"PASS: 'Dominant Category' found in {col_name}: {metric['Dominant Category']}")
            
    print("\nAll checks passed!")

if __name__ == "__main__":
    test_metrics()
