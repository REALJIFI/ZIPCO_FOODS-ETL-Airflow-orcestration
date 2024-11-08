import pandas as pd

#Data Extraction
def run_Extraction():
    try:
        data = pd.read_csv('zipco_transaction.csv')
        print('Data Extracted successfully!')
    except Exception as e:
        print(f"an error occured : {e}")