import pandas as pd

def run_Transformation():
    # Load the data
    data = pd.read_csv('zipco_transaction.csv')
    
    # Remove duplicates
    data.drop_duplicates(inplace=True)
    
    # Handle missing values (filling missing numeric values with mean)
    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        data.fillna({col: data[col].mean()}, inplace=True)
    
    # Handle missing values (where there is string/object with 'unknown')
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data.fillna({col: 'unknown'}, inplace=True)
    
    # Clean data by assigning the correct datatype
    data['Date'] = pd.to_datetime(data['Date'])
    
    # Create dimension tables
    # Products table
    products = data[['ProductName', 'UnitPrice']].drop_duplicates().reset_index(drop=True)
    products.index.name = 'Product_ID'
    products = products.reset_index()
    
    # Customers table
    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers.index.name = 'Customer_ID'
    customers = customers.reset_index()
    
    # Staff table
    Staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    Staff.index.name = 'Staff_ID'
    Staff = Staff.reset_index()
    
    # Create fact table
    Transaction = data.merge(products, on=['ProductName', 'UnitPrice'], how='left')\
                      .merge(customers, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber', 'CustomerEmail'], how='left')\
                      .merge(Staff, on=['Staff_Name', 'Staff_Email'], how='left')
    Transaction.index.name = 'Transaction_ID'
    Transaction = Transaction.reset_index()[[
        'Date', 'Transaction_ID', 'Product_ID', 'Customer_ID', 'Staff_ID', 'Quantity', 'OrderType',
        'StoreLocation', 'PaymentType', 'PromotionApplied', 'Weather', 'Temperature',
        'StaffPerformanceRating', 'CustomerFeedback', 'DeliveryTime_min', 'DayOfWeek', 'TotalSales'
    ]]
    
    # Save data as CSV files
    data.to_csv('clean_data.csv', index=False)
    Transaction.to_csv('Transaction.csv', index=False)
    products.to_csv('products.csv', index=False)
    customers.to_csv('customers.csv', index=False)
    Staff.to_csv('Staff.csv', index=False)
    
    print('Data cleaning and Transformation completed successfully')
