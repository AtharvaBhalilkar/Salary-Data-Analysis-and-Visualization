from dagster import op, Out, In
import pandas as pd

# Helper function to clean the salary column
def clean_salary(salary):
    salary = str(salary).replace("$", "").replace("K", "000").replace(",", "").strip()
    if "-" in salary:
        low, high = map(int, salary.split("-"))
        return (low + high) / 2  # Average of range
    return float(salary)

# Define the transformation operation
@op(ins={'salaries': In(pd.DataFrame)}, out=Out(pd.DataFrame))
def transform_salaries(salaries: pd.DataFrame) -> pd.DataFrame:
    # Rename columns to more descriptive names
    salaries.rename(columns={
        'Field1': 'Job_Title',
        'Field2': 'Location',
        'Field3': 'Salary',
        'Field4': 'Experience',
        'Field5': 'Company_Name'
    }, inplace=True)

    # Clean the Salary column
    salaries['Salary($)'] = salaries['Salary'].apply(clean_salary)

    # Drop the original 'Salary' column
    salaries.drop(columns=['Salary'], inplace=True)

    # Encode experience levels
    experience_mapping = {
        "0-1 years": 1,
        "1-3 years": 2,
        "4-6 years": 3,
        "7-9 years": 4,
        "10-14 years": 5,
        "15+ years": 6
    }

    salaries['Experience_Level'] = salaries['Experience'].map(experience_mapping)
    salaries['Experience_Level'] = salaries['Experience_Level'].fillna(1).astype(int)

    # Extract the state abbreviation (last two characters of the Location column)
    salaries['State'] = salaries['Location'].str[-2:]

    # Define function to assign U.S. regions based on state
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

    # Assign region
    salaries['Region'] = salaries['State'].apply(assign_region)

    # Reorder columns for final output
    salaries = salaries[['Job_Title', 'Company_Name', 'Experience', 'Experience_Level', 'Salary($)', 'Location', 'Region']]

    return salaries
