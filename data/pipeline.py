from data import nodes
from datetime import datetime, timedelta
from constantes import DOWNLOAD_DIR
from os import listdir
from os import listdir
from os.path import isfile, join

def pipeline(path: str) -> str:

    caminho_csv = nodes.csv_download(path, f'{nomeArquivo()}.csv')
    df = nodes.ajust_dataFrame(caminho_csv)
    caminho_parquet = nodes.caminho_parquet(df, f'{nomeArquivo()}.parquet')

    return caminho_parquet


def nomeArquivo() -> str:

    data_referencia = datetime.now()
    dia_semana = data_referencia.weekday()

    if dia_semana == 5:
        data_referencia = data_referencia + timedelta(days=2)
    elif dia_semana == 6:
        data_referencia = data_referencia + timedelta(days=1)
    ano = data_referencia.strftime('%y')

    nome_arquivo = f'IBOVDia_{data_referencia.day:02}-{data_referencia.month:02}-{ano}'

    return nome_arquivo
