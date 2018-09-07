import pandas as pd
df = pd.read_json('data.json')
#add is_duplicated column which marks all but the first occurrence of the url as a duplicate
df['is_duplicated'] = df.duplicated(subset='url')
#query all rows where is_duplicated is True
df.query('is_duplicated==True')
#query the records marked as duplicate
df.query('is_duplicated==True')['url']
#remove dupes
df = df.drop(df.query('is_duplicated==True').index)

#write the data to sql server
from sqlalchemy import create_engine
sqlcon = create_engine('mssql+pyodbc://buzzard_user:aGXnFducpqdAgY86dugYVyZ7KxfPk8@localhost:2017/buzzard?driver=SQL+Server+Native+Client+11.0')
df.to_sql('pd_sites', sqlcon, if_exists='replace')