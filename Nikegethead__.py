#!/usr/bin/env python
# coding: utf-8

# In[70]:


##Work space

## This program takes four csv files from AWS blob storage, manipulate and cleanses it.
## the transformed data it them moves the data back to a bucket in AWS storage.
## The program can be enhanced and extended. 

import boto3
import numpy as np
import pandas as pd
import os
import sys
import pyspark
import datetime
import json

                                    #create bucket variabels
bucket_name = "nikebucket2"
bucket_name2 = "nikedestination"
parth = os.environ["parth"]
filename = "consumption.json"


                                    #start data fetch
#s3 = boto3.resource("s3")
s3 = boto3.client("s3")
s3_resource = boto3.resource('s3')
response = s3.list_objects_v2(Bucket=bucket_name)
df = pd.DataFrame.from_dict(response['Contents'])
obj = s3.get_object(Bucket=bucket_name, Key="source/sales.csv")
sales = pd.read_csv(BytesIO(obj['Body'].read()))
##############################################################################################################
#load the data
#sales call product
response = s3.list_objects_v2(Bucket=bucket_name)
df = pd.DataFrame.from_dict(response['Contents'])
obj = s3.get_object(Bucket=bucket_name, Key="source/product.csv")
product = pd.read_csv(BytesIO(obj['Body'].read()))
###############################################################################################################
response = s3.list_objects_v2(Bucket=bucket_name)
df = pd.DataFrame.from_dict(response['Contents'])
obj = s3.get_object(Bucket=bucket_name, Key="source/store.csv")
store = pd.read_csv(BytesIO(obj['Body'].read()))
###############################################################################################################
response = s3.list_objects_v2(Bucket=bucket_name)
df = pd.DataFrame.from_dict(response['Contents'])
obj = s3.get_object(Bucket=bucket_name, Key="source/calender.csv")
calender = pd.read_csv(BytesIO(obj['Body'].read()))
#################################################################################################### End of data fetch
                            ##display all and check for bad data

#print(sales)
#print(product)
#print(store)
#print(calender)
                                         ##   Start transforming

                                         ##   Transform sales
#drop sales null values
sales =   sales.dropna()
sales[["salesid",  "storeid",  "productid",   "dateid"]] = sales[["salesid","storeid",  "productid",   "dateid"]].astype(int) 
#print(sales)           

                            ##   Start transforming
#################################################################################################
product.dropna()
product[["divisionid",  "gengerid" , "categoryid"]]  = product[["divisionid",  "gengerid",  "categoryid"]].astype(int)
#print(product)
#################################################################################################

                               #Transform store
store = store.dropna()
store[["storeid",  "chanelid"]] = store[["storeid",  "chanelid"]].astype(int)
#print(store)
##################################################################################################

                               #transform calender
#remove null values
calender  = calender.dropna()
calender[["calenderid",   "year" , "week"]] = calender[["calenderid",   "year" , "week"]].astype(int)
#print(calender)
#####################################################################################################

                            #The four tables transformed
    
                             #join or merge cleansed tables 

sales_and_product = pd.merge(sales  ,product , how = "left", left_on="productid",right_on = "productid")
sales_and_store = pd.merge(sales_and_product  ,store , how = "left", left_on="storeid",right_on = "storeid")
sales_and_other_dimensions = pd.merge(sales_and_store ,calender, how = "left", left_on="dateid",right_on = "calenderid")

big_data = sales_and_other_dimensions[["year",  "week",  "salesUnites" , "netSales"]]

                           #aggregate
    
big_data2 = big_data.groupby("week").agg({'salesUnites': "sum", 'netSales': 'sum'})
print(big_data2)

s3 = boto3.resource("s3")
with open(parth+filename, 'rb') as the_file:
    s3.Bucket('nikebucket2').put_object(Key=filename, Body=the_file)
    


#####################  End   ##################################


# In[2]:


import os

os.environ["AWS_ACCESS_KEY_ID"]    = "AKIAIW534WUU557VS4GA"

os.environ["AWS_SECRET_ACCESS_KEY"]  = "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"


client  = boto3.client("s3")
fileName = "testjson.json"
bucket_name5 = "nikebucket2"

json_buffer = StringIO()
df.to_json(json_buffer)
response= s3.put_object(
    ACL="private",
    Body=json_buffer.getvalue(),
    Bucket=bucket_name5,
    key=fileName


# In[ ]:


##Work space

import boto3
import numpy as np
import pandas as pd
#import dask.dataframe as dd
from io import StringIO, BytesIO
import os
import sys
import pyspark
import datetime
#from pyspark.sql.functions import *

                                    #create bucket variabels

bucket_name = "nikebucket2"
bucket_name2 = "nikedestination"


                                    #start data load


s3 = boto3.client("s3")
s3_resource = boto3.resource('s3')
response = s3.list_objects_v2(Bucket=bucket_name)
df = pd.DataFrame.from_dict(response['Contents'])
obj = s3.get_object(Bucket=bucket_name, Key="source/sales.csv")
sales = pd.read_csv(BytesIO(obj['Body'].read()))
##############################################################################################################
#load the data
#sales call product
response = s3.list_objects_v2(Bucket=bucket_name)
response = s3.list_objects_v2(Bucket=bucket_name)
df = pd.DataFrame.from_dict(response['Contents'])
obj = s3.get_object(Bucket=bucket_name, Key="source/product.csv")
product = pd.read_csv(BytesIO(obj['Body'].read()))
###############################################################################################################
response = s3.list_objects_v2(Bucket=bucket_name)
response = s3.list_objects_v2(Bucket=bucket_name)
df = pd.DataFrame.from_dict(response['Contents'])
obj = s3.get_object(Bucket=bucket_name, Key="source/store.csv")
store = pd.read_csv(BytesIO(obj['Body'].read()))

###############################################################################################################
#load the data
#sales call date
response = s3.list_objects_v2(Bucket=bucket_name)
response = s3.list_objects_v2(Bucket=bucket_name)
df = pd.DataFrame.from_dict(response['Contents'])
obj = s3.get_object(Bucket=bucket_name, Key="source/calender.csv")
calender = pd.read_csv(BytesIO(obj['Body'].read()))

###############################################################################################################

##display all and check for bad data

#print(sales)
#print(product)
#print(store)
#print(calender)


print(type(sales_and_store["dateid"]))
print(type(calender["calenderid"]))

newsale[["salesid",  "storeid",  "productid",   "dateid"]] =  newsale[["salesid","storeid",  "productid",   "dateid"]].astype(int) 
print("printinf sale")
print(newsale)



##################################################################################################################Do a join
#df_merge_difkey = pd.merge(df_row, df3, left_on='id', right_on='id')

sales_and_product = pd.merge(sales  ,product , how = "left", left_on="productid",right_on = "productid")

sales_and_store = pd.merge(sales_and_product  ,store , how = "left", left_on="storeid",right_on = "storeid")

sales_and_calender = pd.merge(sales_and_store ,calender, how = "left", left_on="dateid",right_on = "calenderid")

print("print_sales and calene")
print(sales_and_calender)


newsale = df_sales.apply(pd.to_numeric, errors='coerce').dropna(how = "any")

print("newsales")
#print(sales)
sales[["salesid",  "storeid",  "productid",   "dateid"]] = sales[["salesid","storeid",  "productid",   "dateid"]].astype(int) 
print("printinf sale")
print(newsale)



####==========================================================================================
#"salesid",  "storeid",  "productid",   "dateid"

#productid  divisionid  gengerid  categoryid

#type(newsale["netsales"])


#print("printing types")
#print(newsale.dtypes)

#newsalee = newsale[newsale["netSales"].dropna(how="any").astype(int)]

#print(newsale)




#df_sales = pd.DataFrame(data=sales) 

#newsale = df_sales.apply(pd.to_float(), errors='coerce')


#newsale["netsales"] = newsale["netsales"].astype(int) 



print("printing types")
#print(newsale.dtypes)

print("new_way converted")
#print(newew_sales+new2)
#print(new2)
#print(type(newewsales.netSales))


#adition = sales[sales["salesUnites"] + sales["netSales"]] 

#print(adition)
#pd.Series(['123', '42']).astype(float)

#drop  nulls
#product_tr1 = product_tr.dropna(how ="any")
#print("naaaaaa")
#print(product_tr1)
#salefloat = sales.astype(float)

#print(type(sales["salesUnites"]))
#print(type(sales["netSales"]))
#sales2 = sales[sales["salesUnites"].astype(float, copy=True, errors="raise")]
print("sales tra")
#print(sales)
#"salesid",  "storeid",  "productid",   "dateid"
print("addition")




#print(calender_tr)
#print(store_tr)
#print(product_tr)
#print(sales)

#DataFrame.astype(dtype, copy=True, errors=’raise’)

#storeid  chanelid
#print(type(sales))
#print(type(product))
#print(type(store))
#print(type(calender))

#df_calender_trans  =  calender[df[0:1]].copy(deep=True)

#DataFrame.copy(deep=True)[source]¶
#print(df_calender_trans)


#df[df.columns[1:4]] 




#df2.to_csv("s3://nikebucket2/nikedestsiantion1/sales1.csv", index = False)


print("made it to this point")


#print(calender)
#sales2
#print(sales)


# In[61]:


path2 = os.environ["path"] = "C:\\Users\\Begho\\Documents\\Nike_csvs\\local_storage_processed\\"


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[65]:


os.environ["parth"]  = "C:\\Users\\Begho\\Documents\\Nike_csvs\\local_storage_processed\\"


# In[ ]:




