pip install mysqlclient
pip install apache-airflow
export AIRFLOW_HOME=Users/adriandolha/airflow
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=mysql://mragainprod:MrAgain2022@localhost/airflow
airflow db init

