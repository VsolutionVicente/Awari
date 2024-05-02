import boto3
import os
import requests
import pandas as pd
from io import StringIO 


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime



def upload_to_s3():
    # Defina o caminho do arquivo CSV local
    caminho_arquivo = r'downloads/estados.csv'

    # Defina o nome do bucket da Amazon S3 e o nome do arquivo no bucket
    
    nome_arquivo_s3 = 'estados.csv'

    # Configure as credenciais da AWS (você pode usar as suas credenciais)
    nome_bucket = 'awari-de-vicente'
    aws_access_key_id = 'AKIA2UC3FXCDSYRL6VXQ'
    aws_secret_access_key = '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'
    region_name = 'us-east-2'
    endpoint_url= 'https://awari-de-vicente.s3.us-east-2.amazonaws.com/Awari/'

    # Inicialize o cliente S3
    cliente_s3 = boto3.client(
            's3', 
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=None,
            config=boto3.session.Config(signature_version='s3v4'),
            verify=False,
            region_name=region_name
    )

    # Leitura do arquivo CSV com o pandas
    df = pd.read_csv(caminho_arquivo)

    # Converta o DataFrame pandas em um arquivo CSV temporário
    with open('temp.csv', 'w') as temp_file:
        df.to_csv(temp_file, index=False)

    # Envie o arquivo CSV temporário para o S3
    cliente_s3.upload_file(Filename='temp.csv', Bucket=nome_bucket, Key=nome_arquivo_s3)

    # Remova o arquivo temporário
    os.remove('temp.csv')

    print(f'O arquivo {nome_arquivo_s3} foi enviado para o bucket {nome_bucket} da Amazon S3 com sucesso.')

# Defina os argumentos padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Defina o DAG
dag = DAG(
    'upload_to_s3',
    default_args=default_args,
    description='Upload de arquivo para S3',
    schedule_interval=None,
)

# Defina a tarefa usando PythonOperator
upload_to_s3_task = PythonOperator(
    task_id='upload_to_s3_task',
    python_callable=upload_to_s3,
    dag=dag,
)

upload_to_s3_task
