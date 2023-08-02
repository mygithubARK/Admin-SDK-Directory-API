from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery

# Load the credentials
credentials = service_account.Credentials.from_service_account_file (
    'your_credentials.json',
    scopes=['https://www.googleapis.com/auth/admin.directory.group.readonly',
            'https://www.googleapis.com/auth/admin.directory.group.member.readonly',
            'https://www.googleapis.com/auth/bigquery']
)

# Delegated credentials for admin actions
delegated_credentials = credentials.with_subject('admin@domain')

# Create the service client
service = build('admin', 'directory_v1', credentials=delegated_credentials)

# Create a BigQuery client
bq_client = bigquery.Client(credentials=credentials, project='Your_ServiceProjectID')

# Specifying dataset and table
dataset_id = 'Your_BQ_DataSetID'
table_id = 'Your_BQ_TableName'
table_ref = bq_client.dataset(dataset_id).table(table_id)

# Define the schema of your table
schema = [
    bigquery.SchemaField("GroupEmail", "STRING"),
    bigquery.SchemaField("Description", "STRING"),
    bigquery.SchemaField("MemberCount", "INTEGER"),
    bigquery.SchemaField("MemberEmail", "STRING"),
    bigquery.SchemaField("Role", "STRING"),
    bigquery.SchemaField("Type", "STRING"),
    bigquery.SchemaField("Status", "STRING"),
]

# Create a new table or replace existing one
table = bigquery.Table(table_ref, schema=schema)
table = bq_client.create_table(table, exists_ok=True) #Make an API request

# Initialize the rows to be inserted
rows_to_insert = []

# Use the groups.list method in pagination
page_token = None
while True:
    result = service.groups().list(domain='Your_DOMAIN' , pageToken=page_token).execute()
    for group in result.get('groups', []):
        # Get the group details
        group_email = group.get('email', '')
        description = group.get('description', '')
        membersCount = int(group.get('directMembersCount', 0)) # Convert to int

        # Get the members of the group
        member_page_token = None
        while True:
            member_result = service.members().list(groupKey=group_email, pageToken=member_page_token).execute()
            for member in member_result.get('members', []):
                member_email = member.get('email', '')
                member_role = member.get('role', '')
                member_type = member.get('type', '')
                member_status = member.get('status', '')

                # Add to group and member information to the rows to be inserted
                rows_to_insert.append((group_email, description, membersCount, member_email, member_role, member_type, member_status))
                # Write the group and member information to the csv
                # writer.writerow([group_email, description, membersCount, member_email, member_role, member_type, member_status])

            member_page_token = member_result.get('nextPageToken')
            if not member_page_token:
                break
    
    page_token = result.get('nextPageToken')
    if not page_token:
        break

# Insert rows into the BigQuery table
errors = bq_client.insert_rows(table, rows_to_insert) # Make an API request

if errors == []:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))