from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from datetime import datetime, timedelta
from time import sleep

default_args = {
    'start_date': datetime(2020,12,14),
    'owner': 'airflow'
}

def print_context(**context):
    print('-'*21)
    print('context value: ',context)
    sleep(5)
    print('-'*21)

def valentary_error(**context):
    raise Exception("valentary error")

def get_true(**context):
    return True

def get_false(**context):
    return False


def subdag_1(dag_id, default_args):

    with DAG(dag_id, default_args=default_args) as dag:
        sd1_op_1 = PythonOperator(task_id='sd1_op_1', provide_context=True, python_callable=print_context)
        sd1_op_2 = PythonOperator(task_id='sd1_op_2', provide_context=True, python_callable=print_context)
        sd1_op_3 = PythonOperator(task_id='sd1_op_3', provide_context=True, python_callable=print_context)
        sd1_op_4 = PythonOperator(task_id='sd1_op_4', provide_context=True, python_callable=print_context)
        sd1_op_5 = PythonOperator(task_id='sd1_op_5', provide_context=True, python_callable=print_context)
        sd1_op_6 = PythonOperator(task_id='sd1_op_6', provide_context=True, python_callable=print_context)
        sd1_op_7 = PythonOperator(task_id='sd1_op_7', provide_context=True, python_callable=print_context)

        sd1_op_1 >> [sd1_op_2, sd1_op_3]
        sd1_op_6 >> sd1_op_2
        sd1_op_2 >> [sd1_op_4, sd1_op_5]
        sd1_op_5 >> sd1_op_7
        sd1_op_3 >> sd1_op_7

        return dag

with DAG('BasePlayground', 'A playground DAG', default_args=default_args) as dag:
    op_1 = PythonOperator(task_id='op_1', provide_context=True, python_callable=print_context)
    op_2 = PythonOperator(task_id='op_2', provide_context=True, python_callable=print_context)
    op_3 = PythonOperator(task_id='op_3', provide_context=True, python_callable=print_context)
    op_4 = PythonOperator(task_id='op_4', provide_context=True, python_callable=print_context)
    op_5 = PythonOperator(task_id='op_5', provide_context=True, python_callable=print_context)
    op_6 = PythonOperator(task_id='op_6', provide_context=True, python_callable=print_context)
    op_7 = PythonOperator(task_id='op_7', provide_context=True, python_callable=print_context)
    op_8 = PythonOperator(task_id='op_8', provide_context=True, python_callable=print_context)
    op_9 = PythonOperator(task_id='op_9', provide_context=True, python_callable=print_context)
    op_10 = PythonOperator(task_id='op_10', provide_context=True, python_callable=print_context)
    op_edge_2 = PythonOperator(task_id='op_edge_2', provide_context=True, python_callable=print_context)
    op_edge_5 = PythonOperator(task_id='op_edge_5', provide_context=True, python_callable=print_context)
    op_edge_6 = PythonOperator(task_id='op_edge_6', provide_context=True, python_callable=print_context)
    op_edge_7 = PythonOperator(task_id='op_edge_7', provide_context=True, python_callable=print_context)
    op_final = PythonOperator(task_id='op_final', provide_context=True, python_callable=print_context)
    sd_1_dag_id = dag.dag_id + '.sd_1'
    sd_1 = SubDagOperator(subdag=subdag_1(sd_1_dag_id, default_args), 
                            task_id='sd_1')
    sd_2_dag_id = dag.dag_id + '.sd_2'
    sd_2 = SubDagOperator(subdag=subdag_1(sd_2_dag_id, default_args), 
                            task_id='sd_2')
    sd_3_dag_id = dag.dag_id + '.sd_3'
    sd_3 = SubDagOperator(subdag=subdag_1(sd_3_dag_id, default_args), 
                            task_id='sd_3')
    sd_4_dag_id = dag.dag_id + '.sd_4'
    sd_4 = SubDagOperator(subdag=subdag_1(sd_4_dag_id, default_args), 
                            task_id='sd_4')
    sd_5_dag_id = dag.dag_id + '.sd_5'
    sd_5 = SubDagOperator(subdag=subdag_1(sd_5_dag_id, default_args), 
                            task_id='sd_5')

    [op_1, op_edge_2] >> op_2 >> [op_3, op_4, op_5] >> op_6 >> op_7
    [op_6, op_8] >> op_9
    op_4 >> op_10
    [op_7, op_9, op_10] >> op_final
    op_5 >> op_edge_5
    op_6 >> op_edge_6
    op_7 >> op_edge_7
    op_2 >> sd_1
    op_5 >> [sd_2, sd_4] 
    sd_4 >> sd_5 >> op_7
    op_6 >> sd_3
    sd_2 >> sd_3

