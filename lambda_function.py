import boto3
import datetime
import subprocess
from send_email import send_email
import json

def lambda_handler(event, context):
    # Set up S3 connection with Lambda
    s3_client = boto3.client('s3')

    # Specify bucket and folder names in S3
    bucket_name = "wcd-midterm-s3"
    folder_name = 'raw'

    # Create an empty list to store the names of the objects
    s3_file_list = []

    # List objects in the folder
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    # Append object keys to the list
    for obj in response['Contents']:
        key = obj['Key']
        if key != folder_name and key != folder_name + '/':
            s3_file_list.append(key.replace(folder_name, '', 1).lstrip('/'))

    # Remove the first empty item from the list
    if s3_file_list and s3_file_list[0] == '':
        s3_file_list = s3_file_list[1:]

    # Print the contents of the list
    print("s3_file_list:", s3_file_list)

    # Set the desired timezone
    desired_timezone = tz.gettz('America/New_York')

    # Get the current date in the desired timezone
    now = datetime.datetime.now()
    datestr = now.astimezone(desired_timezone).strftime("%Y-%m-%d")

    # List the required objects to be in the S3 bucket
    #required_file_list = [f'sales_{datestr}.csv.gz']
    required_file_list = [f'calendar_{datestr}.csv.gz', f'inventory_{datestr}.csv.gz', f'product_{datestr}.csv.gz', f'sales_{datestr}.csv.gz', f'store_{datestr}.csv.gz']
    print("required_file_list: ", required_file_list)

    if set(s3_file_list) == set(required_file_list):
        s3_file_url = ['s3://' + 'wcd-midterm-s3' + '/raw/' + a for a in required_file_list]  # Construct s3_file_url using required_file_list
        table_name = [a[:-18] for a in required_file_list]  # Construct table_name using required_file_list
        print(s3_file_list, table_name)

        # Set up json.dumps configuration as per requirements from Airflow
        data = json.dumps({'conf': {a: b for a, b in zip(table_name, s3_file_url)}})  # Use zip() to combine table_name and s3_file_url
        print(data)

        # Send data to Airflow
        endpoint = 'http://ec2-3-87-187-193.compute-1.amazonaws.com:8080/api/v1/dags/midterm_dag/dagRuns'  # Amend Airflow URL as per the EC2 Public IP
        subprocess.run(['curl', '-X', 'POST', endpoint, '-H', 'Content-Type: application/json', '--user', 'klailatimad:password', '-d', data])

        print('Data sent to Airflow')
    else:
        # send_email()
        print("email sent")
