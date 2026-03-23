import sys
import re
import boto3
from functools import reduce
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Inicialização
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job.init(args["JOB_NAME"], args)

RAW_PATH = "s3://b3-tech-challenge-ricardo/raw/"
REFINED_PATH = "s3://b3-tech-challenge-ricardo/refined/"

# Configuração para sobrescrever partições dinamicamente
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# 1) Listar arquivos via Boto3 para evitar erro de "dataset não encontrado"
s3_client = boto3.client('s3')
bucket = RAW_PATH.replace("s3://", "").split("/")[0]
prefix = "/".join(RAW_PATH.replace("s3://", "").split("/")[1:])

response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
files = [f"s3://{bucket}/{obj['Key']}" for obj in response.get(
    'Contents', []) if obj['Key'].endswith('.parquet')]

if not files:
    raise ValueError(f"Nenhum arquivo .parquet encontrado em {RAW_PATH}")


def normalize_and_standardize(path):
    """Lê um arquivo individual e padroniza as colunas."""
    df = spark.read.parquet(path)
    all_cols = df.columns

    # Identifica colunas (case-insensitive e ignorando aspas extras)
    def pick(candidates):
        for cand in candidates:
            for c in all_cols:
                if cand.lower() in c.lower().replace("'", ""):
                    return c
        return None

    col_date = pick(["Date"])
    col_close = pick(["Close"])
    col_volume = pick(["Volume"])
    col_ticker = pick(["ticker"])

    # Ticker via path (fallback robusto)
    # Extrai o nome do arquivo antes do .parquet
    ticker_match = re.search(r"/([^/]+)\.parquet", path)
    ticker_from_path = ticker_match.group(1) if ticker_match else "UNKNOWN"

    # Seleção com tratamento de tipos
    return df.select(
        F.to_timestamp(F.col(f"`{col_date}`")).alias("dt_referencia"),
        F.col(f"`{col_close}`").cast("double").alias("vlr_fechamento"),
        F.col(f"`{col_volume}`").cast("double").alias("qtd_volume"),
        (F.col(f"`{col_ticker}`") if col_ticker else F.lit(
            ticker_from_path)).alias("ticker")
    ).withColumn(
        "ticker",
        F.when(F.col("ticker").isNull() | (F.trim(F.col("ticker")) == ""),
               F.lit(ticker_from_path)).otherwise(F.col("ticker"))
    )


# 2) Processamento em loop e Union
list_dfs = [normalize_and_standardize(f) for f in files]
df_std = reduce(lambda df1, df2: df1.unionByName(
    df2, allowMissingColumns=True), list_dfs)

# 3) Cálculos de indicadores
window_spec = Window.partitionBy("ticker").orderBy("dt_referencia")

df_calc = (
    df_std
    .filter(F.col("dt_referencia").isNotNull())
    .withColumn("ticker", F.trim(F.col("ticker")))
    .withColumn(
        "retorno_diario",
        (F.col("vlr_fechamento") - F.lag("vlr_fechamento", 1).over(window_spec)) /
        F.lag("vlr_fechamento", 1).over(window_spec)
    )
    .withColumn(
        "media_movel_5d",
        F.avg("vlr_fechamento").over(window_spec.rowsBetween(-4, 0))
    )
)

# 4) Particionamento e Escrita
df_final = (
    df_calc
    .withColumn("year", F.year("dt_referencia"))
    .withColumn("month", F.month("dt_referencia"))
)

(
    df_final.write
    .mode("overwrite")
    .partitionBy("year", "month", "ticker")
    .parquet(REFINED_PATH)
)

job.commit()
