Steps to start airflow:
    
    Run mkdir -p ./dags ./logs ./plugins ./config
        echo -e "AIRFLOW_UID=$(id -u)" > .env
    
    Run docker compose up airflow-init
    
    Run docker compose up

Accessing the web interface:
    The webserver is available at: http://localhost:8080. The default account has the login airflow and the password airflow.


To clean airflow env: 

    Run docker compose down --volumes --remove-orphans command in the directory you downloaded the docker-compose.yaml file

    Remove the entire directory where you downloaded the docker-compose.yaml file rm -rf '<DIRECTORY>'

    Run through this guide from the very beginning, starting by re-downloading the docker-compose.yaml file

    docker compose down --volumes --rmi all

