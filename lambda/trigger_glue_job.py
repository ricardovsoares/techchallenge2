import boto3
import os

glue = boto3.client('glue')


def lambda_handler(event, context):
    JOB_NAME = os.getenv("JOB_NAME")

    if not JOB_NAME:
        raise ValueError("A variável de ambiente JOB_NAME não foi definida.")

    try:
        response = glue.start_job_run(JobName=JOB_NAME)
        return {
            'statusCode': 200,
            'body': f"Glue Job {JOB_NAME} iniciado. RunId: {response['JobRunId']}"
        }
    except Exception as e:
        print(e)
        raise e
