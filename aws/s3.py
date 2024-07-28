from aws.session import session
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

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

def s3_verify_duplicate(bucket_name: str):
    response = s3.meta.client.list_objects(Bucket=bucket_name, Prefix='refined/')

    data_referencia = datetime.now()
    dia_semana = data_referencia.weekday()
    if dia_semana == 5:
        data_referencia = data_referencia + timedelta(days=2)
    elif dia_semana == 6:
        data_referencia = data_referencia + timedelta(days=1)

    data = data_referencia.strftime("%Y-%m-%d")

    for obj in response.get('Contents', []):
        if data in obj['Key']:
            return False
    return True
