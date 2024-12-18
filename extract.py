from dagster import op, Out, In
from pymongo import MongoClient
import pandas as pd
import os

# MongoDB connection string
mongo_connection_string = "mongodb://localhost:27017/"

# Define the extract operation
@op(out=Out(pd.DataFrame))
def extract_salaries() -> pd.DataFrame:
    # Connect to MongoDB
    client = MongoClient(mongo_connection_string)
    db = client['DAP']
    collection = db['Semi_Data_Salary']
    
    # Extract data from the collection and load into a DataFrame
    salaries_data = pd.DataFrame(list(collection.find({})))
    salaries_data.drop(columns=["_id"], inplace=True)  # Drop the MongoDB internal _id column
    
    # Close the connection
    client.close()
    
    return salaries_data
