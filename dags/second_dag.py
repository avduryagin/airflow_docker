from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

dag=DAG('SECOND_dag',
        start_date=days_ago(0,0,0,0,0)
        )
operation=BashOperator(
        bash_command= "pwd",
        dag=dag,
        task_id='operation_2'


)

#comment

operation