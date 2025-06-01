Steps to start:
    Activate virtual env:
        python -m venv venv
        source venv/bin/activate
    Install airflow in venv:
        pip install --upgrade pip
        pip install apache-airflow
    

Accessing the web interface:
    The webserver is available at: http://localhost:8080. The default account has the login airflow and the password airflow.


To clean airflow env: 

    Run docker compose down --volumes --remove-orphans command in the directory you downloaded the docker-compose.yaml file

    Remove the entire directory where you downloaded the docker-compose.yaml file rm -rf '<DIRECTORY>'

    Run through this guide from the very beginning, starting by re-downloading the docker-compose.yaml file

    docker compose down --volumes --rmi all

