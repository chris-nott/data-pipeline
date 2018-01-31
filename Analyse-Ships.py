
# coding: utf-8

# Credentials to connect to the Cloudant database that holds the AIS data collected by Node-RED. You can generate a set of credentials for cloudant host, username and password from the IBM Cloud -> Data & Analytics -> Service Credentials tab. Or simply get DSX to generate the code for you from the Catalog service in the Watson Data Platform. (Check the url.)

# In[12]:


# @hidden_cell
credentials_1 = {
  'password':"<enter>",
  'custom_url':'<enter>',
  'username':'<enter>'
}


# Create an Apache Spark session to work with.

# In[13]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# We can now import the AIS data received into a Node-RED flow and stored in Cloudant into a Spark database for bulk analysis.

# In[14]:


cloudantdata = spark.read.format("org.apache.bahir.cloudant").option("cloudant.host",credentials_1['custom_url']).option("cloudant.username", credentials_1['username']).option("cloudant.password",credentials_1['password']).option("jsonstore.rdd.partitions",'5').load("ships")


# Let's just check that the data has come across OK by checking the schema that the import has automatically chosen for us...

# In[15]:


cloudantdata.printSchema


# Now the data is in Apache Spark, we can count the number of instances very quickly:

# In[16]:


cloudantdata.count()


# Let's look at the top three rows of data (json documents).

# In[17]:


cloudantdata.head(3)


# Now we create a subset of the dataframe, taking the lat and lon coordinates of ships's positions.

# In[18]:


shipCoords = cloudantdata.select('lat','lon')
shipCoords.show(4)


# By converting this lat/lon dataframe to a pandas dataframe, we can manipulate the data set more easily because of a wider range of available functions.  We ensure that the lat and lon data values are all floats, and records for which either value throws an error are discarded.  The final function converts the dataframe into an array format.

# In[9]:


import pandas as pd
pd_df = shipCoords.toPandas()
pd_df.apply(lambda x: pd.to_numeric(x, errors='coerce')).dropna()
pd_df.as_matrix()


# Iterate through the dataframe to build an list of ship coordinates.  Take the first 100 positions so that we don't obliterate the grid we shall display them on.

# In[10]:


positions = []
for index, row in pd_df.iterrows():
    positions.append([row[0],row[1]])

positions = positions[0:100]
print positions


# Display the ship positions on a graph.

# In[11]:


import matplotlib.pyplot as plt

for point in positions:
    plt.scatter(point[0],point[1])

plt.show()
