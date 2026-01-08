# Python Libraries
import os
import urllib.parse

#External Libraries
from sqlalchemy import create_engine, text, inspect
import pandas as pd
from dotenv import load_dotenv


# Step 1: Define the connection parameters
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


# URL encode password in case it contains special chars
password_encoded = urllib.parse.quote_plus(DB_PASSWORD)

# Create global engine once
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{password_encoded}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
output_folder = f"CSV files/{DB_NAME}"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)


# Get list of table names
inspector = inspect(engine)
tables = inspector.get_table_names()

print("Your Database Contains the following Tables:")
for table in tables:
    print(tables.index(table) + 1,table)



def exporter():
    # location to store files
    output_folder = f"CSV files/{DB_NAME}"
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # looping through the entire database tables   
    for table in tables:
        print(f"Exporting {table}...")
        chunksize = 5000

        # Define the file path for the CSV
        csv_file_path = os.path.join(output_folder, f"{table}.csv")


        #Appending for larger records
        for i, chunk in enumerate(pd.read_sql(f"SELECT * FROM {table}", engine, chunksize=chunksize)):
            if i == 0:
                chunk.to_csv(csv_file_path, index=False, mode='w')
            else:
                chunk.to_csv(csv_file_path, index=False, mode='a', header=False)
    print("All tables exported!")


while True:
    action = input("Proceed with exporting? Y/N: ")
    if action.lower() in ("y", "yes"):
        exporter()
        break
    elif action.lower() in ("no", "n"):
        print("Thank you for your time")
        break
    else:
        continue


