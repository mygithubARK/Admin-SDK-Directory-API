from google.oauth2 import service_account
from googleapiclient.discovery import build
import csv

# Load the credentials
credentials = service_account.Credentials.from_service_account_file (
    'your_credentials.json',
    scopes=['https://www.googleapis.com/auth/admin.directory.group.readonly',
            'https://www.googleapis.com/auth/admin.directory.group.member.readonly']
)

# Delegated credentials for admin actions
delegated_credentials = credentials.with_subject('admin@domain')

# Create the service client
service = build('admin', 'directory_v1', credentials=delegated_credentials)

# Open the CSV file
with open('gcpgroups-members.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    # Writing the headers
    writer.writerow(['GroupEmail' , 'Description', 'MembersCount', 'Member Email', 'Role', 'Type', 'Status'])

    # Use the groups.list method with pagination
    page_token = None
    while True:
        result = service.groups().list(domain='yourDomainName' , pageToken=page_token).execute()
        for group in result.get('groups', []):
            # Get the group details
            group_email = group.get('email', '')
            description = group.get('description', '')
            membersCount = group.get('directMembersCount', '')

            # Get the members of the group
            member_page_token = None
            while True:
                member_result = service.members().list(groupKey=group_email, pageToken=member_page_token).execute()
                for member in member_result.get('members', []):
                    member_email = member.get('email', '')
                    member_role = member.get('role', '')
                    member_type = member.get('type', '')
                    member_status = member.get('status', '')

                    # Write the group and member information to the csv
                    writer.writerow([group_email, description, membersCount, member_email, member_role, member_type, member_status])

                member_page_token = member_result.get('nextPageToken')
                if not member_page_token:
                    break
    
        page_token = result.get('nextPageToken')
        if not page_token:
            break