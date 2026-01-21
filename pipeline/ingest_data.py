#!/usr/bin/env python
# coding: utf-8

# In[29]:


import pandas as pd

year = 2021
month = 1




# In[30]:


# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz')


# In[31]:


# Display first rows
df.head()


# In[32]:


# Check data types
df.dtypes


# In[33]:


# Check data shape
df.shape


# In[18]:


df


# In[35]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    dtype=dtype,
    parse_dates=parse_dates
)


# In[36]:


df


# In[37]:


df.head()


# In[38]:


df['tpep_pickup_datetime']


# In[39]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[40]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[41]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[42]:


len(df)


# In[44]:


url = prefix + f'yellow_tripdata_{year}-{month}.csv.gz'


# In[59]:


df_iter = pd.read_csv(
    url,
    dtype=dtype,
    parse_dates=parse_dates,
    iterator=True,
    chunksize=100000,
)


# In[60]:


df_chunk = next(df_iter)


# In[61]:


from tqdm.auto import tqdm


# In[62]:


for df_chunk in tqdm(df_iter):
    df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
    print(len(df_chunk))


# In[ ]:




