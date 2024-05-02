from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from hello_word_operator import HelloWordOperator

def helloWord():
    print("Hello Word Vicente")

#Criar uma DAG
with DAG(dag_id="dag_hello",
          start_date = datetime(2024,1,1),
          schedule_interval ="@hourly",
          catchup=False) as dag:

        # criar uma task
        tarefa1 = PythonOperator(
              task_id = "ola",
              python_callable=helloWord)
        
        tarefa2 = HelloWordOperator(task_id="primeiro-operador-tarefa", name="mundo com operador" )

#Chamado a tarefa
tarefa1 >> tarefa2