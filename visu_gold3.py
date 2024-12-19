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

# Load the data from Golden Layer 3
gold_3_df = pd.DataFrame(session.execute("SELECT * FROM Golden_Layer3"))

# Create a scatter plot
plt.figure(figsize=(10, 6))
for location in gold_3_df['location'].unique():
    loc_data = gold_3_df[gold_3_df['location'] == location]
    plt.scatter(loc_data['bmi'], loc_data['heart_disease'], label=location)

plt.title("BMI vs Heart Disease by Location")
plt.xlabel("BMI")
plt.ylabel("Heart Disease Cases")
plt.legend(title="Location", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.show()
