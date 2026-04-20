import json
import boto3

def lambda_handler(event, context):
    # Get file details from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    size = event['Records'][0]['s3']['object']['size']
    
    print(f"File received: {key} in bucket: {bucket}, size: {size} bytes")
    
    # Validate the file
    if not key.endswith('.csv'):
        print(f"ERROR: File {key} is not a CSV file!")
        return {
            'statusCode': 400,
            'body': json.dumps(f'Invalid file type: {key}')
        }
    
    if size == 0:
        print(f"ERROR: File {key} is empty!")
        return {
            'statusCode': 400,
            'body': json.dumps(f'Empty file: {key}')
        }
    
    print(f"Validation passed for {key}")
    
    # Trigger Glue ETL job
    glue_client = boto3.client('glue', region_name='us-east-2')
    
    response = glue_client.start_job_run(
        JobName='cms-etl-transform'
    )
    
    job_run_id = response['JobRunId']
    print(f"Glue job started successfully! Run ID: {job_run_id}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Validation passed. Glue job started: {job_run_id}')
    }