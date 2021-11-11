import json
import mysql.connector as mysql
import requests
import pandas as pd
import pymysql
from urllib.parse import quote
from sqlalchemy.engine import create_engine

# sqlalchemy connector - please provide your actual connection details
engine = create_engine(
    'mysql+pymysql://[User]:%s@[Host]:3306/[Database]' % quote('[Password]'))

# requirement 1 - create dataset of 4500 users
response = requests.get("https://randomuser.me/api/?results=4500")
data = response.json()
df = pd.json_normalize(data["results"], max_level=3)

# requirement 2 - Split the dataset to 2 gender datasets(male, female) and store it in MySql
df_male = df[df["gender"] == "male"]
df_female = df[df["gender"] == "female"]
df_male.to_sql(name="Asaf_Ben_Eliezer_test_male", con=engine, index=False, if_exists='append')
df_female.to_sql(name="Asaf_Ben_Eliezer_test_female", con=engine, index=False, if_exists='append')


# requirement 3&4 - Split the dataset to 10 subsets by dob.age column in groups of 10 and store it
def create_group_subsets(df):
    for i in range(1, 11):
        subset = df[(df["dob.age"] >= i * 10) & (df["dob.age"] < (i + 1) * 10 - 1)]
        subset.to_sql(name=f'Asaf_Ben_Eliezer_test_{i}', con=engine, index=False, if_exists='append')


create_group_subsets(df)

