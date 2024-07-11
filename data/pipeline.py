from data import nodes
from datetime import datetime

def pipeline(path: str) -> str:

    caminho_csv = nodes.csv_download(path, f'{nomeArquivo()}.csv')
    df = nodes.ajust_dataFrame(caminho_csv)
    caminho_parquet = nodes.caminho_parquet(df, f'{nomeArquivo()}.parquet')

    return caminho_parquet

def nomeArquivo() -> str:
    now = datetime.now()
    ano = now.strftime('%y')
    nomeArquivo = f'IBOVDia_{now.day:02}-{now.month:02}-{ano}'
    return nomeArquivo