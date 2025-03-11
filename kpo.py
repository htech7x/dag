from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago

# Define the DAG
dag = DAG(
    'example_kubernetes_pod_operator',
    default_args={'owner': 'airflow'},
    schedule_interval=None,  # Run manually
    start_date=days_ago(1),
    catchup=False,
)

# Define the KubernetesPodOperator Task
kpo_task = KubernetesPodOperator(
#    namespace='test',  # Change this to your Airflow namespace
#    kubernetes_conn_id="k8s_conn",
#    service_account_name="alex",
    image='python:3.9-slim',  # Container image to use
    cmds=["python", "-c"],
    arguments=["import time; time.sleep(20); print('Hello from KPO!')"],  # Simple Python script
    name="kpo-task",
    task_id="run_python_script",
    get_logs=True,  # Capture logs from the pod
    dag=dag,
)
task_2 = KubernetesPodOperator(
    image="python:3.9",
    cmds=["python", "-c"],
    arguments=["import time; time.sleep(10); print('Hello from KPO task 2')"], 
    name="example-task-2",
    task_id="run-in-k8s-2",
    dag=dag,
)
task_3 = KubernetesPodOperator(
    image="python:3.9",
    cmds=["python", "-c"],
    arguments=["import time; time.sleep(10); print('Hello from KPO task 3')"], 
    name="example-task-3",
    task_id="run-in-k8s-3",
    dag=dag,
)

