# importing necessary libraries
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os 

def run_loading():

    #loading the data set
    data = pd.read_csv(r'cleanedata.csv')
    products = pd.read_csv(r'products.csv')
    customers = pd.read_csv(r'customers.csv')
    Staff = pd.read_csv(r'Staff.csv')
    Transaction= pd.read_csv(r'Transaction.csv')

        # Data loading to azure
    # load the environment variables from .env
    load_dotenv()
    connect_str = os.getenv('CONNECT_STR')
    container_name = os.getenv('CONTAINER_NAME')

    # Create Blobservice client object

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    # load data into azure blob storage
    files = [
        (data, 'rawdata/clean_zipco_Transaction_data.csv'), # creating a folder inside the created contanier
        (products, 'cleandata/products.csv'),
        (customers, 'cleandata/customers.csv'),
        (Staff, 'cleandata/Staff.csv'),
        (Transaction, 'cleandata/Transaction.csv'),
    ]

    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f'{blob_name} loaded into azure storage')

