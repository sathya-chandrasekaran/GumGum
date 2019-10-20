
# coding: utf-8

# In[2]:


import pyspark
import os
import sys


# In[3]:


from pyspark.sql import SparkSession


# In[4]:


spark = SparkSession     .builder     .appName("transform_adevents")     .getOrCreate()


# In[5]:


adevents_df = spark.read.json("/Users/murugannatatajan/Documents/gumgum/adevents.json")


# In[6]:


adevents_df.printSchema()


# In[7]:


adevents_df.count()


# In[8]:


sc = spark.sparkContext


# In[9]:


from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead, first, last


# In[10]:


windowSpec = Window.partitionBy(adevents_df['visitorId']).orderBy(adevents_df['timestamp'].asc())


# In[11]:


adevents_df.groupBy('visitorId').count().orderBy('count',ascending=False).show(10,False)


# In[12]:


df_new = adevents_df.withColumn("nextPageUrl",lead('pageUrl').over(windowSpec))


# In[13]:


df_new.write.format('json').mode("overwrite").save("/Users/murugannatatajan/Documents/gumgum/user_adjourney")


# In[14]:


df_new.coalesce(1).write.format('json').mode("overwrite").save("/Users/murugannatatajan/Documents/gumgum/user_event_adjourney")


# In[15]:


df_new.printSchema()


# In[16]:


df_op = df_new.select("id","timestamp","type","visitorId","pageUrl","nextPageUrl")


# In[17]:


df_op.coalesce(1).write.format('json').mode("overwrite").save("/Users/murugannatatajan/Documents/gumgum/user_journey_webpage")


# In[20]:


df_op.count()


# In[21]:


df_op.printSchema()

