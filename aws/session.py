import boto3
from constantes import aws_access_key_id, aws_secret_access_key, aws_session_token

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token)