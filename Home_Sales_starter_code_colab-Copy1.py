#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
# Find the latest version of spark 3.x  from http://www.apache.org/dist/spark/ and enter as the spark version
# For example:
# spark_version = 'spark-3.5.0'
spark_version = 'spark-3.5.0'
os.environ['SPARK_VERSION']=spark_version

# Install Spark and Java
get_ipython().system('apt-get update')
get_ipython().system('apt-get install openjdk-11-jdk-headless -qq > /dev/null')
get_ipython().system('wget -q http://www.apache.org/dist/spark/$SPARK_VERSION/$SPARK_VERSION-bin-hadoop3.tgz')
get_ipython().system('tar xf $SPARK_VERSION-bin-hadoop3.tgz')
get_ipython().system('pip install -q findspark')

# Set Environment Variables
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = f"/content/{spark_version}-bin-hadoop3"

# Start a SparkSession
import findspark
findspark.init()


# In[2]:


# Import packages
from pyspark.sql import SparkSession
import time

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


# In[3]:


# 1. Read in the AWS S3 bucket into a DataFrame.
from pyspark import SparkFiles
url = "https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv"


# In[4]:


# 2. Create a temporary view of the DataFrame.
df.CreateReplaceTempView('home_sales')


# In[5]:


# 3. What is the average price for a four bedroom house sold in each year rounded to two decimal places?

 spark.sql("""select year (date) as year,
             round(avg(price),2) as Average_Price
             from home_sales
             where bedrooms == 4
             group by year
             order by year desc""").show()
 


# In[6]:


# 4. What is the average price of a home for each year the home was built
# that have 3 bedrooms and 3 bathrooms rounded to two decimal places?
 spark.sql("""select date_built as year,
             round(avg(price),2) as Average_Price
             from home_sales
             where (bedrooms == 3) and (bathrooms == 3)
             group date_built
             order by date_built desc""").show()


# In[7]:


# 5. What is the average price of a home for each year built that have 3 bedrooms, 3 bathrooms, with two floors,
# and are greater than or equal to 2,000 square feet rounded to two decimal places?
 spark.sql("""select date_built as year,
             round(avg(price),2) as Average_Price
             from home_sales
             where (bedrooms == 3) and (bathrooms == 3) and (sqft_living >=2000) and (floors = 2)
             group by date_built
             order by date_built desc""").show()


# In[8]:


# 6. What is the "view" rating for the average price of a home, rounded to two decimal places, where the homes are greater than
# or equal to $350,000? Although this is a small dataset, determine the run time for this query.
start_time = time.time()
 spark.sql("""select view as View_Rating,
             round(avg(price),2) as Average_Price
             from home_sales
             group by view having avg(price)>=350000
             order by view desc""").show()
print("--- %s seconds ---" % (time.time() - start_time))


# In[9]:


# 7. Cache the the temporary table home_sales.
spark.sql("cache table home_sales ")


# In[10]:


# 8. Check if the table is cached.
spark.catalog.isCached('home_sales')


# In[11]:


# 9. Using the cached data, run the query that filters out the view ratings with average price
#  greater than or equal to $350,000. Determine the runtime and compare it to uncached runtime.

start_time = time.time()
spark.sql("""select view as View_Rating,
             round(avg(price),2) as Average_Price
             from home_sales
             group by view having avg(price)>=350000
             order by view desc""").show()


print("--- %s seconds ---" % (time.time() - start_time))


# In[12]:


# 10. Partition the home sales dataset by date_built field.
df.write.partitionBy('data_built').parquet('home_sales_partitioned').mode('overwrite')


# In[13]:


# 11. Read the parquet formatted data.
p_df_parquet=spark.read.parquet('home_sales_partitioned')


# In[14]:


# 12. Create a temporary table for the parquet data.
p_df_parquet.CreateReplaceTempView('parquet_home_sales')


# In[15]:


# 13. Run the query that filters out the view ratings with average price of greater than or equal to $350,000
# with the parquet DataFrame. Round your average to two decimal places.
# Determine the runtime and compare it to the cached version.

start_time = time.time()
spark.sql("""select view as View_Rating,
             round(avg(price),2) as Average_Price
             from parquet_home_sales
             group by view having avg(price)>=350000
             order by view desc""").show()


print("--- %s seconds ---" % (time.time() - start_time))


# In[16]:


# 14. Uncache the home_sales temporary table.
spark.sql('uncache table home_sales')


# In[17]:


# 15. Check if the home_sales is no longer cached
if spark.catalog.isCached('home_sales'):
  print('home_sales is still cached')
else:
  print('all clear')


# In[17]:




