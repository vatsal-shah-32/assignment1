import pandas as pd 
import ijson   # ijson is one of best library for json flie reader in python 
from pandas import json_normalize  # extract dictionary data in Json file 
import copy  # do whole file copy and save 
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.DEBUG)

# Initialize an empty list to store the push JSON data
logging.debug(f"json file readed in python and seperated by _ ")
data_list = []

# Open the large JSON file
with open('C://Users//HP//Downloads//Wolke K8//input.json', 'r') as file:  # given here you input file path
    # Parse JSON objects one by one using ijson
    for i in ijson.items(file, 'data.item'):
        data_list.append(i)


data = json_normalize(data_list, sep='_')     # Flatten the JSON structure and create a DataFrame

logging.info(f"json file readed successfully...")

logging.debug(f"Now Datafile chcek start in basic data information")
print("~> Rows and Columns in Data : ")
print(data.shape)
data.info()
print("~> Checking Null Value in Data : ")
print(data.isnull().sum().sum())
print(data.dropna(inplace=True))
print("~> Removing Null Value in After Data : ")
print(data.isnull().sum().sum())
data.head()
print("~> Column Names in Data : ")
print(data.columns.to_list())
logging.debug(f"Datafile basic inforamrion checking complete...")

# create a new merge column
logging.debug(f"Generating new Merge Colunn")
data['resource/id'] = data[['resource_environment_id','resource_id','resource_display_name']].apply(lambda x: ' || '.join(x.astype(str)), axis=1)
logging.info(f"New recource/id column are generated...")

# arranged columns by output file
logging.debug(f"Arranged column by output file ")
new_column_order = ['resource_environment_id','resource_id','resource_display_name', 'start_date', 'end_date','product','quantity', 'unit', 'amount','original_amount','discount_amount','price','resource/id','line_type' ]
data  = data[new_column_order]
logging.info(f"Arranged completed and file is ready...")

# now new generate column base on  line_type column data and added usage and discount 
# so first I create a deep copy of this file to copy and save another dataframe 
logging.debug(f"New generate column base on  line_type column data and added usage and discount ")
data2 = copy.deepcopy(data)

data.loc[:, 'line_type'] = 'Usage'
data2.loc[:,'line_type'] = 'Discount'
logging.info(f"two dataframe is ready and also putvalues in line_type column...")

# creating cost column based on discount rows.
logging.debug(f"creating cost column based on discount rows.")
data2['original_amount'] = data2['discount_amount'] * -1

data = data.drop(columns=['discount_amount'])
data2 = data2.drop(columns=['discount_amount'])
logging.info(f"discount_amount column is delete in both dataframe and regenrate perfect original_amount...")

# now make a new merge dataframe base on  data dataframe and data2 dataframe 
logging.debug(f"make a new dataframe in merge both above dataframe")
df = pd.concat([data,data2]).sort_index(kind= 'merge')
df.reset_index(drop= True, inplace= True)
logging.info(f"new dataframe df is ready by row wise...")

# Convert 'start_date' and 'end_date' columns to datetime objects
df['start_date'] = pd.to_datetime(df['start_date'])
df['end_date'] = pd.to_datetime(df['end_date'])

# Now calculate the duration column 
logging.debug(f"calucate duration column")
df['duration'] = (df['end_date'] - df['start_date'])/pd.Timedelta(hours=1)

df['duration'] = df['duration'].astype(int)

df = df.drop(columns=['price'])
logging.info(f"calculated duration column is ready...")

# calculation of quantity
logging.debug(f"calucate quantity column")
df['quantity'] = (df['quantity'])/(df['duration']) 

df['quantity'] = df['quantity'].round(2)
logging.info(f"calculated quantity column is ready...")

# arranged columns by output file 
logging.debug(f"again arranged column by output file ")
new_column_order = ['resource_environment_id','resource_id','resource_display_name', 'start_date', 'end_date','duration','product','quantity', 'unit', 'amount','original_amount','resource/id','line_type' ]
df = df[new_column_order]
logging.info(f"again arranged completed and file is ready...")

# amount column
logging.debug(f"calucate amount column")
df['amount'] = (df['amount']/24)
df['amount'] = df['amount'].round(4)
logging.info(f"calculated amount column is ready...")

# set original amount column 
logging.debug(f"recalucate original_amount column")
df['original_amount'] = df['original_amount']/24
logging.info(f"recalculated original_amount column is ready...")

# change date formate 
logging.debug(f"change dateformate")
df['start_date'] = df['start_date'].dt.strftime('%m-%d-%Y')
df['end_date'] = df['end_date'].dt.strftime('%m-%d-%Y')
logging.info(f"dateformate is successfully changed...")

# Re-running the adjusted code to handle the case when the time is missing from the dates

# Define the function to generate hourly intervals
logging.debug(f"generate hourly intervals wise rows")
def generate_hourly_intervals(df:pd.DataFrame, start_date, end_date)->pd.DataFrame:
    """Generate hourly intervals between start_date and end_date."""
    # Add default time if not present
    if len(start_date) == 10:  # Format: 'MM-DD-YYYY'
        start_date += 'T00:00:00'
    if len(end_date) == 10:  # Format: 'MM-DD-YYYY'
        end_date += 'T00:00:00'

    # Parse the dates
    start = datetime.strptime(start_date, '%m-%d-%YT%H:%M:%S')
    end = datetime.strptime(end_date, '%m-%d-%YT%H:%M:%S')

    # Create a list of hourly intervals
    hourly_intervals = []
    current_time = start
    while current_time < end:
        next_time = current_time + timedelta(hours=1)
        hourly_intervals.append((current_time.strftime('%m-%d-%YT%H:%M:%S'), next_time.strftime('%m-%d-%YT%H:%M:%S')))
        current_time = next_time

    return hourly_intervals

# Create an empty DataFrame to store the new rows
new_rows = []

# Iterate through each row in the DataFrame
for index, row in df.iterrows():
    start_date = row['start_date']
    end_date = row['end_date']

    # Generate hourly intervals for the current row
    hourly_intervals = generate_hourly_intervals(df,start_date, end_date)

    # Append a new row for each hourly interval
    for interval in hourly_intervals:
        new_rows.append({
            'resource_environment_id': row['resource_environment_id'],
            'resource_id': row['resource_id'],
            'resource_display_name': row['resource_display_name'],
            'start_date': interval[0], # Add hourly start_date
            'end_date': interval[1],    # Add hourly end_date
            'duration' : row['duration'],
            'product' : row['product'],
            'quantity' : row['quantity'],
            'unit' : row['unit'],
            'amount': row['amount'],
            'original_amount': row['original_amount'],
            'resource/id' : row['resource/id'],
            'line_type' : row['line_type']
           
        })
logging.info(f"generate hourly intervals wise rows are successfully...")

logging.debug(f"new_df is generated")
# Create a new DataFrame with the generated rows
new_df = pd.DataFrame(new_rows)

# Display the first few rows of the new DataFrame for verification
logging.info(f"new_df is generated successfully...")
new_df

new_df.to_csv("final_generated_rows.csv", index=False)