Steps to start:
    Activate virtual env:
        python -m venv venv
        source venv/bin/activate
    Install packages in venv:
        pip install -r requirements.txt
    

Accessing Airflow web interface:
    The webserver is available at: http://localhost:8080. The default account has the login "admin" and the password "admin".

To run:
python spark/src/main.py 
    OR
spark-submit --master spark://localhost:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.mongodb.spark:mongo-spark-connector_2.12:10.5.0  spark/src/main.py 


