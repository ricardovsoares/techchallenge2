import yfinance as yf
import pandas as pd
import boto3
from datetime import datetime
import io

# Configurações
BUCKET_NAME = "b3-tech-challenge-ricardo"
TICKERS = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "^BVSP"]
DATE_STR = datetime.now().strftime("%Y-%m-%d")


def ingest_data():
    s3 = boto3.client('s3')

    for ticker in TICKERS:
        print(f"Coletando dados para: {ticker}")
        # Coleta dados do último ano para ter histórico para média móvel
        df = yf.download(ticker, period="1y", interval="1d")
        df.reset_index(inplace=True)
        df['ticker'] = ticker

        # Buffer para salvar em Parquet
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)

        # Caminho particionado por data de extração
        path = f"raw/extraction_date={DATE_STR}/{ticker}.parquet"

        s3.put_object(Bucket=BUCKET_NAME, Key=path, Body=buffer.getvalue())
        print(f"Upload concluído: {path}")


if __name__ == "__main__":
    ingest_data()
