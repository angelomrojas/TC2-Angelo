from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from constantes import DOWNLOAD_DIR

def csv_download(path: str, nomeArquivo: str):

    caminho = f'{DOWNLOAD_DIR}/{nomeArquivo}'

    if os.path.exists(caminho):
        return caminho

    chrome_options = Options()
    prefs = {"download.default_directory": DOWNLOAD_DIR}
    chrome_options.add_experimental_option("prefs", prefs)

    chrome_service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
    
    driver.get(path)
    time.sleep(2.5)
    driver.find_element(By.CSS_SELECTOR, "#segment > option:nth-child(2)").click()
    time.sleep(.5)
    driver.find_element(By.CSS_SELECTOR, ".list-avatar-row .primary-text a").click()
    time.sleep(2.5)
    driver.quit()

    return caminho

def ajust_dataFrame(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, encoding='latin-1',header=1, sep=';', index_col=False)
    
    mask = ~df['Código'].isnull()
    df = df[mask]
    df['Ação'] = df['Ação'].str.replace('/', '')

    df['Part. (%)Acum.'] = df['Part. (%)Acum.'].str.replace(',', '.')
    df['Part. (%)Acum.'] = df['Part. (%)Acum.'].astype(float)

    df['Part. (%)'] = df['Part. (%)'].str.replace(',', '.')
    df['Part. (%)'] = df['Part. (%)'].astype(float)

    df['Qtde. Teórica'] = df['Qtde. Teórica'].str.replace('.', '')
    df['Qtde. Teórica'] = df['Qtde. Teórica'].astype(np.int64)

    df['data_carga'] = datetime.now().date()

    df = df.rename(columns={'Qtde. Teórica':'qtd', 'Part. (%)':'participacao',
                            'Part. (%)Acum.':'participacao_acumulada', 'Ação':'acao'})

    return df

def caminho_parquet(df: pd.DataFrame, nomeArquivo: str) -> str:

    caminho = f'{DOWNLOAD_DIR}/{nomeArquivo}'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, caminho, coerce_timestamps='ms', allow_truncated_timestamps=True)

    return caminho

