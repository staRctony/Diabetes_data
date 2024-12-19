from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

# Load secrets from the token file
with open("/Users/sudhir/Desktop/Fall2024/BIGDATA/Project/Sudhir-token (1).json") as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

cloud_config = {
    'secure_connect_bundle': 'secure-connect-sudhir (3).zip'}

auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)

cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

# Check connection
if session:
    print("Connection is active")
else:
    print("Connection is not active")


# Set the keyspace
session.set_keyspace('diabetes')


import matplotlib.pyplot as plt
import pandas as pd

# Load the data from Golden Layer 2
gold_2_df = pd.DataFrame(session.execute("SELECT * FROM Golden_Layer2"))

# Count the number of diabetes cases for each gender
diabetes_counts = gold_2_df['gender'].value_counts()

# Plot the pie chart
diabetes_counts.plot(kind='pie', autopct='%1.1f%%', figsize=(8, 8), startangle=140)
plt.title("Distribution of Diabetes Cases by Gender")
plt.ylabel("")  # Hide the y-axis label
plt.tight_layout()
plt.show()
