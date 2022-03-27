#!/usr/bin/env python
# coding: utf-8

# In[4]:


pip list


# ## Import packages to import data 

# In[5]:


import pandas as pd
import boto3
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np


# ## Ingest data from Amazon s3 bucket

# In[6]:


client = boto3.client('s3')


# In[7]:


path = 's3://cloud-compute-ads-508/Online Retail.csv'


# In[8]:


path


# In[9]:


retail_df = pd.read_csv(path)


# In[10]:


retail_df


# In[11]:


retail_df.head(6),retail_df.shape


# In[ ]:





# In[12]:


path_1 = 's3://cloud-compute-ads-508/amazon_co-ecommerce_sample.csv'


# In[13]:


path_1


# In[14]:


amazon_ecom = pd.read_csv(path_1)


# In[15]:


amazon_ecom


# In[16]:


amazon_ecom.head(4),amazon_ecom.shape


# In[ ]:





# In[17]:


amazon_ecom.head(10)


# In[18]:


retail_df.describe()


# In[ ]:





# In[19]:


path_2 = 's3://cloud-compute-ads-508/customers.csv'


# In[20]:


cust_df = pd.read_csv(path_2)


# In[21]:


cust_df


# In[22]:


cust_df.head(5),cust_df.shape


# ## Counting missing valules of the three dataframes

# In[ ]:





# In[23]:


#viewing missing data 
cust_df.isnull()


# In[24]:


cust_df.isnull().sum()


# In[25]:


retail_df.isnull().count()


# In[26]:



retail_df.isnull().sum()


# In[27]:


amazon_ecom.isnull().sum()


# # Checking the unnamed columns

# In[28]:


amazon_ecom.isnull().count().head(20)


# In[29]:


# Creating a new column without unnamed columns
amazon_1 = pd.DataFrame(amazon_ecom, columns=['uniq_id','product_name','manufacturer','price','number_available_in_stock','number_of_reviews',
           'number_of_answered_questions','average_review_rating','amazon_category_and_sub_category','customers_who_bought_this_item_also_bought',
           'description','product_information','product_description','items_customers_buy_after_viewing_this_item','customer_questions_and_answers',
           'customer_reviews','sellers'])


# In[154]:


amazon_1.info()


# In[153]:





# In[31]:


amazon_1.isnull().sum(),amazon_1.shape


# In[32]:


amazon_1['product_name'].value_counts()


# In[33]:


amazon_1['average_review_rating'].value_counts()


# In[163]:


# reviewing the missing nan values
amazon_1['average_review_rating'].value_counts(dropna=False)


# In[ ]:





# In[35]:


amazon_1['number_of_reviews'].value_counts(dropna=False)


# In[36]:


# viewing the null values in the data frame
amazon_1[amazon_1['number_of_reviews'].isnull()]


# In[37]:


# Looking at not null values
amazon_1.notnull().head(6)


# # Fill in missing values

# In[38]:


amazon_1['average_review_rating'].fillna(value = '2.3 out of 5 stars', inplace=True)


# In[39]:


amazon_1['average_review_rating'].value_counts(dropna=False)


# In[40]:


# counting missing values of retail data
retail_df.isnull().sum()


# ## Look at the dataset data types

# In[41]:


retail_df.info();cust_df.info();amazon_ecom.info()


# In[42]:


cust_df.tail(5)


# In[43]:


cust_df.filter(regex='ACTIVE')


# # Graphically looking at cust_df

# In[44]:


retail_df


# In[45]:


retail_df.dtypes


# # converting InvoiceDate to datetime

# In[ ]:





# In[47]:


retail_1 = retail_df.astype({'Country': 'string'})
print(retail_1.dtypes)


# In[48]:


#viewing sales by country
sns.barplot(x='Quantity', y='Country', data=retail_1)


# In[49]:


# Viewing density plot of unit price
retail_1.UnitPrice.plot.density(color='red')


# In[50]:


#Look at the unit price for each country
retail_1.groupby('Country').UnitPrice.value_counts().unstack()


# In[149]:


#listing columns of dataframe
(amazon_ecom.columns)


# In[52]:


# counting null values
amazon_ecom.isnull().sum(axis=0)


# In[53]:


# counting missing values
amazon_ecom.isnull().sum(axis=1)


# In[54]:


# get column names
print(amazon_ecom.columns)


# In[55]:


# looking at descriptive statistics
amazon_ecom.describe()


# In[56]:


# filtering the columns
amazon_ecom.filter([ 'product_name', 'manufacturer', 'price',
       'number_available_in_stock', 'number_of_reviews',
       'number_of_answered_questions', 'average_review_rating',
       'amazon_category_and_sub_category',
       'customers_who_bought_this_item_also_bought']).count()


# In[ ]:





# ## Looking at the data graphically

# In[57]:


import matplotlib.pyplot as plt
import seaborn as sns


# In[58]:


amazon_1.columns


# In[59]:


data = amazon_1.groupby('number_of_reviews')


# In[60]:


data


# In[61]:


for number_of_reviews, amazon_1 in data:
    print('5.0 out of 5 stars')
    print(amazon_1)


# In[147]:


sns.countplot(x= 'average_review_rating',data=amazon_1)


# In[148]:


sns.barplot(x= 'average_review_rating', y = 'product_name', data =amazon_1)


# In[ ]:





# In[69]:


amazon_ecom.info()


# In[70]:


amazon_ecom1 = amazon_ecom[['number_of_reviews', 'product_name']]


# In[71]:


type(amazon_ecom1)
print(amazon_ecom1.dtypes)


# In[75]:


amazon_plot = sns.barplot(y = 'number_of_reviews', x= 'product_name', data=amazon_1, saturate=1)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[76]:


import sagemaker


# In[137]:


sess = sagemaker.Session()
bucket = sess.default_bucket()
role = sagemaker.get_execution_role()
region = boto3.Session().region_name


# In[138]:


s3_staging_dir = "s3://{0}/athena/staging".format(bucket)


# In[ ]:





# In[ ]:





# In[ ]:





# In[139]:


conn = connect(region_name=region, s3_staging_dir=s3_staging_dir)


# In[140]:


amazon_ecom.corr


# ## List all the files in the s3 bucket

# In[81]:


get_ipython().system('aws s3 ls s3://cloud-compute-ads-508')


# ## look at the public buckets

# In[82]:


get_ipython().system('aws s3 ls s3://cloud-compute-ads-508/amazon_co-ecommerce_sample.csv')


# In[83]:


get_ipython().system('aws s3 ls s3://cloud-compute-ads-508/Online Retail.csv')


# In[84]:


get_ipython().system('aws s3 ls s3://cloud-compute-ads-508/customers.csv')


# ## Set Public s3 bucket to a Private s3 bucket

# In[ ]:





# In[85]:


# set public bucket to private bucket
s3_public_path_csv = 's3://cloud-compute-ads-508'


# In[86]:


get_ipython().run_line_magic('store', 's3_public_path_csv')


# ## Set s3 private location

# In[87]:


s3_private_path_csv = 's3://{}/cloud-compute-ads-508'.format(bucket)
print(s3_private_path_csv)


# In[88]:


get_ipython().run_line_magic('store', 's3_private_path_csv')


# ## Copied data from public s3 bucket to our private s3 bucket in the account

# In[89]:


get_ipython().system("aws s3 cp --recursive $s3_public_path_csv/ $s3_private_path_csv/ --exclude '*' --include 'amazon_co-ecommerce_sample.csv'")
get_ipython().system("aws s3 cp --recursive $s3_public_path_csv/ $s3_private_path_csv/ --exclude '*' --include 'Online Retail.csv'")
get_ipython().system("aws s3 cp --recursive $s3_public_path_csv/ $s3_private_path_csv/ --exclude '*' --include 'customers.csv'")


# # list files in the private s3 bucket 

# In[90]:


print(s3_private_path_csv)


# In[91]:


get_ipython().system('aws s3 ls $s3_private_path_csv/')


# In[92]:


get_ipython().run_line_magic('store', '')


# In[ ]:





# In[ ]:





# ## Import Pyathena and add a connection

# In[93]:


import sagemaker


# In[94]:


from pyathena import connect


# In[95]:


sess = sagemaker.Session()
bucket = sess.default_bucket()
role = sagemaker.get_execution_role()
region = boto3.Session().region_name

account_id = boto3.client("sts").get_caller_identity().get("Account")

sm = boto3.Session().client(service_name="sagemaker", region_name=region)


# In[96]:


ingest_create_athena_table_tsv_passed = False


# In[97]:


get_ipython().run_line_magic('store', '-r ingest_create_athena_db_passed')


# In[ ]:





# In[98]:


if not ingest_create_athena_db_passed:
    print("++++++++++++++++++++++++++++++++++++++++++++++")
    print("[ERROR] YOU HAVE TO RUN ALL PREVIOUS NOTEBOOKS.  You did not create the Athena Database.")
    print("++++++++++++++++++++++++++++++++++++++++++++++")
else:
    print("[OK]")


# In[ ]:





# In[ ]:





# In[ ]:





# In[99]:


# set up a staging directory what athena use for temporary data and processing a table
s3_staging_dir = 's3://{0}/athena/staging'.format(bucket)


# In[100]:


conn = connect(region_name=region, s3_staging_dir=s3_staging_dir)


# ## Create an Athena Database

# In[101]:


#Create Database
db_name='market_basket'


# In[102]:


statement = "CREATE DATABASE IF NOT EXISTS {}".format(db_name)
print(statement)


# In[103]:


pd.read_sql(statement, conn)


# ## Verfify Database Creation

# In[104]:


amz_tbl = 'Show Databases'

df_show = pd.read_sql(amz_tbl, conn)
df_show.head(5)


# In[105]:


# set Athena parameters
db_name = 'market_basket'
table_name_csv = 'amz_tbl'


# In[106]:


get_ipython().run_line_magic('store', '')


# In[107]:


pd.read_sql(statement, conn)


# # verify table has been created 

# In[108]:


df_show = pd.read_sql(statement, conn)
df_show.head(3)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[109]:


conn = connect(region_name=region, s3_staging_dir=s3_staging_dir)


# In[110]:


df_show = pd.read_sql(statement, conn)
df_show.head(3)


# In[ ]:





# ## Create a table in Athena 

# In[111]:


statement = """CREATE TABLE IF NOT EXISTS {}.{}(
    `uniq_id` int,
    `product_name` string,
    `manufacturer` string,
    `price` int,
    `number_available_in_stock` int,
    `number_of_reviews` int,
    `number_of_answered_questions` int,
    `average_review_rating` float,
    `amazon_category_and_sub_category` string,
    `customers_who_bought_this_item_also_bought` string,
    `description` string,
    `product_information` string,
    `product_description` string,
    `items_customers_buy_after_viewing_this_item` string,
    `customer_questions_and_answers` string,
    `customer_reviews` string,
    `sellers` string         
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' LOCATION '{}'
TBLPROPERTIES ('skip.header.line.count'='1')""".format(
    db_name, table_name_csv, s3_private_path_csv
)

print(statement)


# In[112]:


import pandas as pd

pd.read_sql(statement, conn)


# In[113]:


statement = "show Tables in {}".format(db_name)

df_show = pd.read_sql(statement, conn)
df_show.head(5)


# In[114]:


get_ipython().run_line_magic('store', '')


# ## Run SQL queries 

# In[115]:


#select * from amz_tbl limit2


# In[116]:


import sys
get_ipython().system('{sys.executable} -m pip install pyathena')


# In[118]:


df_2 = pd.read_sql("SELECT * FROM {}.{} LIMIT 8".format(db_name, table_name_csv), conn)


# In[119]:


df_2


# In[129]:


manufacturer = 'Generic'

statement = """select * from {}.{}
    WHERE manufacturer = 'Generic' limit 100""".format(
    db_name, table_name_csv, manufacturer
)

print(statement)


# In[130]:


df_1 = pd.read_sql(statement, conn)
df_1


# In[ ]:





# ## Checking to see if the data was loaded properly

# In[131]:


if not df_1.empty:
    print('[ok]')
else:
    print('_________________')
    print ('[error] your data was not registered with athena')


# # Create an athena table

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




