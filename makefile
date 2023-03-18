AIRFLOW_REPLICAS=1
AIRFLOW_VERSION=2.1.4
AIRFLOW_HOME=$(HOME)/airflow
AIRFLOW__CORE__SQL_ALCHEMY_CONN=<put here airflow db connection string>
AIRFLOW_DAGS="$(HOME)/airflow/dags"
NEO_HOME=/Users/adriandolha/neo/data

install-airflow:
	export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	export AIRFLOW__CORE__SQL_ALCHEMY_CONN=$(AIRFLOW__CORE__SQL_ALCHEMY_CONN)  && \
	pip install "apache-airflow[celery, mysql]==2.5.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.2/constraints-3.9.txt"

airflow-initdb:
	export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	export AIRFLOW__CORE__SQL_ALCHEMY_CONN=$(AIRFLOW__CORE__SQL_ALCHEMY_CONN)  && \
	airflow db init

# Add admin user
airflow-create-admin-user:
	export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	export AIRFLOW__CORE__SQL_ALCHEMY_CONN=$(AIRFLOW__CORE__SQL_ALCHEMY_CONN)  && \
	airflow users create -u admin -p admin -f admin -l admin -r Admin -e admin@mailhog.local

# Start airflow web
airflow-web:
	export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	export AIRFLOW__CORE__SQL_ALCHEMY_CONN=$(AIRFLOW__CORE__SQL_ALCHEMY_CONN)  && \
	airflow webserver -p 8080

# Install pipeline code that will be used in airflow pipeline
install-uniprot:
	cd app && \
	pip install -r requirements.txt  && \
	python setup.py install

# Copy updated dag to airflow dags folder
install-dag:
	cp -r airflow/* $(AIRFLOW_HOME)

# Generate new xml files to trigger the pipeline
generate-data:
	python -c "from uniprot import pipeline; pipeline.generate_data(1)"

# Start airflow scheduler
airflow-scheduler:
	export AIRFLOW_HOME=$(AIRFLOW_HOME) && \
	export AIRFLOW__CORE__SQL_ALCHEMY_CONN=$(AIRFLOW__CORE__SQL_ALCHEMY_CONN)  && \
	airflow scheduler

# Start neo4j (no auth)
neo4j:
	docker run \
      --publish=7474:7474 --publish=7687:7687 \
      --volume=$(NEO_HOME)/data:/data \
      --env=NEO4J_AUTH=none \
      neo4j:latest
