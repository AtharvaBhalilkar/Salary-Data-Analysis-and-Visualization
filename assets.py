from dagster import op, Out, In
from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine


# MongoDB connection string
MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
POSTGRES_CONNECTION_STRING = 'postgresql://postgres:root@localhost:5432/Salaries_DAP'


# Extract operation
@op(out=Out(pd.DataFrame))
def extract_salaries() -> pd.DataFrame:
    """Extract data from MongoDB into a DataFrame."""
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client['DAP']
    collection = db['Semi_Data_Salary']
    
    # Extract and prepare data
    salaries_data = pd.DataFrame(list(collection.find({})))
    salaries_data.drop(columns=["_id"], inplace=True)  # Drop the MongoDB internal _id column
    client.close()
    
    return salaries_data


# Transform operation
def clean_salary(salary):
    """Clean the salary column values."""
    salary = str(salary).replace("$", "").replace("K", "000").replace(",", "").strip()
    if "-" in salary:
        low, high = map(int, salary.split("-"))
        return (low + high) / 2  # Average of range
    return float(salary)


@op(ins={"salaries": In(pd.DataFrame)}, out=Out(pd.DataFrame))
def transform_salaries(salaries: pd.DataFrame) -> pd.DataFrame:
    """Transform data for analysis."""
    # Rename columns for clarity
    salaries.rename(columns={
        'Field1': 'Job_Title',
        'Field2': 'Location',
        'Field3': 'Salary',
        'Field4': 'Experience',
        'Field5': 'Company_Name'
    }, inplace=True)

    # Transform salary data
    salaries['Salary($)'] = salaries['Salary'].apply(clean_salary)
    salaries.drop(columns=['Salary'], inplace=True)

    # Map experience levels
    experience_mapping = {
        "0-1 years": 1,
        "1-3 years": 2,
        "4-6 years": 3,
        "7-9 years": 4,
        "10-14 years": 5,
        "15+ years": 6
    }
    salaries['Experience_Level'] = salaries['Experience'].map(experience_mapping).fillna(1).astype(int)

    # Extract state information
    salaries['State'] = salaries['Location'].str[-2:]

    # Assign regions
    def assign_region(state):
        northeast = ["ME", "NH", "VT", "MA", "RI", "CT", "NY", "NJ", "PA"]
        south = ["DE", "MD", "DC", "VA", "WV", "NC", "SC", "GA", "FL", 
                 "KY", "TN", "AL", "MS", "AR", "LA", "OK", "TX"]
        midwest = ["OH", "IN", "IL", "MI", "WI", "MN", "IA", "MO", "ND", "SD", "NE", "KS"]
        west = ["MT", "ID", "WY", "NV", "UT", "CO", "AZ", "NM", "WA", "OR", "CA", "AK", "HI"]

        if state in northeast:
            return "Northeast"
        elif state in south:
            return "South"
        elif state in midwest:
            return "Midwest"
        elif state in west:
            return "West"
        else:
            return "Other"

    salaries['Region'] = salaries['State'].apply(assign_region)

    # Select relevant fields for further processing
    salaries = salaries[['Job_Title', 'Company_Name', 'Experience', 'Experience_Level', 'Salary($)', 'Location', 'Region']]

    return salaries


# Load operation
@op(ins={"transformed_salaries": In(pd.DataFrame)})
def load_salaries(transformed_salaries: pd.DataFrame):
    """Load data into PostgreSQL."""
    engine = create_engine(POSTGRES_CONNECTION_STRING)
    
    try:
        transformed_salaries.to_sql('salarytable', engine, if_exists='replace', index=False)
        print("Data loaded successfully into the database.")
    except Exception as e:
        print(f"Error: {e}")

