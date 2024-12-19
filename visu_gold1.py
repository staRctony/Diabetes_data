
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



# Set the keyspace
session.set_keyspace('diabetes')

from cassandra.cluster import Cluster
import pandas as pd
import matplotlib.pyplot as plt

# Load the data from Golden Layer 1
rows = session.execute("SELECT year, gender, blood_glucose_level FROM Golden_Layer1")
gold_1_df = pd.DataFrame(rows)

# Debug: Print the columns to ensure correctness
print("Columns in DataFrame:", gold_1_df.columns)

# Ensure the DataFrame has the expected columns
if not {'year', 'gender', 'blood_glucose_level'}.issubset(gold_1_df.columns):
    print("Expected columns not found in DataFrame. Check the query or data.")
else:
    # Group by year and gender, and calculate the average blood glucose level
    gold_1_grouped = gold_1_df.groupby(['year', 'gender'])['blood_glucose_level'].mean().unstack()

    # Plot the bar chart
    gold_1_grouped.plot(kind='bar', figsize=(10, 6))
    plt.title("Average Blood Glucose Level by Gender for Each Year")
    plt.xlabel("Year")
    plt.ylabel("Average Blood Glucose Level")
    plt.legend(title="Gender")
    plt.tight_layout()
    plt.show()


