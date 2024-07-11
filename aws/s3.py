from aws.session import session
from botocore.exceptions import ClientError

s3 = session.resource('s3')

def s3_buckets_names():
    nome = ''
    for x in s3.buckets.all():
        nome = x.name
    return nome

def s3_upload(caminho_arquivo: str, bucket_name: str, object_name: str) -> str:
    try:
        s3.meta.client.upload_file(caminho_arquivo, bucket_name, object_name)
    except ClientError as e:
        return 'Não foi realizado o upload do objeto!'
    return 'Realizado upload do objeto!'