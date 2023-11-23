import requests
import json
import io
from google.cloud import bigquery

# ServiceNow instance details
base_url = "https://dev182970.service-now.com"
username = "rahamath.nalband"
password = "Ransr197*"

# Table name and API endpoint
table_name = "cmdb_ci"
endpoint = f"/api/now/table/{table_name}"



# Parameters for pagination
chunk_size = 60000 # Adjust the chunk size as per your needs
offset = 0
exclude_ref_link = True
#limit = 10

# Initialize an empty list to store the data
all_records = []

while True:
    # Create a session and add authentication headers
    session = requests.Session()
    session.auth = (username, password)

    headers = {
      'Accept': 'application/json',
         
    }

    # Define query parameters for pagination
    params = {
        "sysparm_limit": chunk_size,
        "sysparm_offset": offset,
       # "sysparm_query": 'active=true'
        #"sysparm_limit":  limit
        "sysparm_exclude_reference_link" : exclude_ref_link
    }

    # Make a GET request to retrieve data
#response = session.get(base_url + endpoint, params=params)
    print (f"Making call to Servicenow {base_url + endpoint} ")
    response = requests.get(base_url + endpoint, headers=headers, auth=(username, password), params=params)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        records = data.get("result", [])
        all_records.extend(records)

        if len(records) < chunk_size:
            # If there are fewer records than the chunk size, we have retrieved all data
            break 
        offset += chunk_size
        print (f"offset == {offset}")
    else:
           print(f"Failed to retrieve data. HTTP status code: {response.status_code}")
           break



# Save the data to a JSON file
with open(f"{table_name}.json", "w") as json_file:
    json.dump(all_records, json_file, indent=2)
print("Data exported successfully!")

#Loading data into BigQuery


ndjson_list = [json.dumps(obj) for obj in all_records]
ndjson_str = '\n'.join(ndjson_list)

with open(f'{table_name}.ndjson', 'w') as file:
    file.write(ndjson_str)

#json_data = ""
#for record in all_records:
 #   json_data += json.dumps(record) + "\n"

#json_data = [json.dumps(rec) for rec in json.load(all_records)] ;

# Initialize a BigQuery client
client = bigquery.Client(project="sunlit-flag-397402")

# Specify your BigQuery dataset and table
dataset_id = 'Servicenow'
table_id =  table_name

# Define the schema based on your ServiceNow data
schema = [
    bigquery.SchemaField('documentkey', 'STRING'),
    bigquery.SchemaField('user', 'STRING'),
    bigquery.SchemaField('record_checkpoint', 'STRING'),
    bigquery.SchemaField('sys_created_by', 'STRING'),
    bigquery.SchemaField('tablename', 'STRING'),
    bigquery.SchemaField('reason', 'STRING'),
    bigquery.SchemaField('newvalue', 'STRING'),
    bigquery.SchemaField('internal_checkpoint', 'STRING'),
    bigquery.SchemaField('fieldname', 'STRING'),
    bigquery.SchemaField('sys_id', 'STRING'),
    bigquery.SchemaField('sys_created_on', 'TIMESTAMP'),



    # Add more fields as needed
]


# Create the table if it doesn't exist
table_ref = client.dataset(dataset_id).table(table_id)
#table = bigquery.Table(table_ref, schema=schema)
#table = client.create_table(table, exists_ok=True)  # The exists_ok parameter creates the table only if it doesn't exist

print ("Loading data to BigQuery started" )

#json_data=json.loads(ndjson_str)

schema_update_options = [
    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
]

print(ndjson_str)
# Load data into the table
#job_config = bigquery.LoadJobConfig(schema=schema)
job_config = bigquery.LoadJobConfig(autodetect=True,
                                    schema_update_options=schema_update_options,source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
#job = client.load_table_from_json(ndjson_str, table_ref, job_config=job_config)

#job = client.load_table_from_file('output.ndjson', table_ref, job_config=job_config)


json_file_path = f"/Users/rahamath.nalband/Documents/GCP-BigQuery/{table_id}.ndjson"
with open(json_file_path, "rb") as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        job_config=job_config
    )
job.result()


# Configure the job to load data from a JSON file
#job_config = bigquery.LoadJobConfig(
#    schema=schema,  # Automatically detect the schema
#    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
#) 
#job = client.load_table_from_json(ndjson_str, table_ref, job_config=job_config)
#job.result() 

print ("Loading data  to BigQuery completed")

