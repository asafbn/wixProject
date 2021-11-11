import json
import mysql.connector as mysql
import pandas as pd
import pymysql
from urllib.parse import quote
from sqlalchemy.engine import create_engine

# sqlalchemy connector - please provide your actual connection details
engine = create_engine(
    'mysql+pymysql://[User]:%s@[Host]:3306/[Database]' % quote('[Password]'))

# requirement 5 - Write a sql query that will return the top 20 last registered males and females and store it
df_20 = pd.read_sql('(SELECT * FROM interview.Asaf_Ben_Eliezer_test_male order by `registered.date` DESC limit 20) '
                    'UNION (SELECT * FROM interview.Asaf_Ben_Eliezer_test_female order by `registered.date` DESC '
                    'limit 20)', con=engine)

df_20.to_sql(name="Asaf_Ben_Eliezer_test_20", con=engine, index=False, if_exists='append')

# requirement 6 - combine data from table 20 & table 5, each row presented once - json write in the end of file
df_5 = pd.read_sql('SELECT * FROM interview.Asaf_Ben_Eliezer_test_5', con=engine)
# drop duplicate make it distinct data
df_5_20_distinct = pd.concat([df_5, df_20]).drop_duplicates().reset_index(drop=True)

# requirement 7 - combine data from table 20 & table 2, no distinct data - json write in the end of file
df_2 = pd.read_sql('SELECT * FROM interview.Asaf_Ben_Eliezer_test_2', con=engine)
df_2_20 = pd.concat([df_2, df_20]).reset_index(drop=True)


# this func used to Inverse of Pandas json_normalize column separated by '.' to nested json
def df_to_formatted_json(df, sep="."):
    result = []
    for idx, row in df.iterrows():
        parsed_row = {}
        for col_label, v in row.items():
            keys = col_label.split(".")

            current = parsed_row
            for i, k in enumerate(keys):
                if i == len(keys) - 1:
                    current[k] = v
                else:
                    if k not in current.keys():
                        current[k] = {}
                    current = current[k]
        # save
        result.append(parsed_row)
    return result


with open('./first.json', 'w') as f:
    json.dump(df_to_formatted_json(df_5_20_distinct), f)

with open('./second.json', 'w') as f:
    json.dump(df_to_formatted_json(df_2_20), f)
