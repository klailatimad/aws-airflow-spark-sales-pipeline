import boto3
import time
import subprocess
from send_email import send_email
import json


def lambda_handler(event,context):
    s3_file_list = []
    
    # s3_client=boto3.client('s3')
    # for object in s3_client.list_objects_v2(Bucket='wcd-midterm-s3')['Contents']:
    #     s3_file_list.append(object['Key'])
    # print("s3_file_list: ", s3_file_list)
    
    #amendement 
    s3_client = boto3.client('s3')
    folder_name = 'raw/'
    
    s3_file_list = []
    
    response = s3_client.list_objects_v2(Bucket='wcd-midterm-s3')
    for object in response.get('Contents', []):
        key = object['Key']
        if key.startswith(folder_name) and not key.endswith('/'):
            s3_file_list.append(key[len(folder_name):])  # Exclude folder name from the key
            
    print("s3_file_list:", s3_file_list)


    #to here
    
    datestr = time.strftime("%Y-%m-%d")
    
    #required_file_list = [f'calendar_{datestr}.csv', f'inventory_{datestr}.csv', f'product_{datestr}.csv', f'sales_{datestr}.csv', f'store_{datestr}.csv']
    required_file_list = [f'sales_2023-05-10.csv.gz'] #testing list #amend 2023-05-10 with {datestr}
    print("required_file_list: ", required_file_list)
    
    # scan S3 bucket
    if set(s3_file_list)==set(required_file_list):
        s3_file_url = ['s3://' + 'wcd-midterm-s3' + '/raw/' + a for a in s3_file_list] #check if it is //raw// or /raw/
        table_name = [a[:-18] for a in s3_file_list]
        print(s3_file_list, table_name)
    
        data = json.dumps({'conf':{a:b for a in table_name for b in s3_file_url}})
        # send signal to Airflow
        print(data)
        endpoint= 'http://ec2-3-86-252-249.compute-1.amazonaws.com:8080/api/v1/dags/midterm_dag/dagRuns'
    
        subprocess.run(['curl', '-X', 'POST', endpoint, '-H', 'Content-Type: application/json', '--user','username:password', '-d', data])
        print('Files are sent to Airflow')
        
    else:
        #send_email()
        print("email sent")
