from data.pipeline import pipeline, nomeArquivo
from aws.s3 import s3_upload, s3_buckets_names, s3_upload, s3_verify_duplicate
from constantes import url


if __name__ == '__main__':

    caminho_parquet = pipeline(url)
    bucket_name = s3_buckets_names()
    object_name = 'raw/IBOVDia.parquet'
    if s3_verify_duplicate(bucket_name):
        print(s3_upload(caminho_parquet, bucket_name, object_name))
    else:
        print("Data já carregada no S3!")


