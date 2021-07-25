# data-pipeline-apache-airflow
This is a project submission as part of Udacity assignment to create ETL pipeline using Apache Airflow. The DAGs are constructed for ETL flow to load and transform song
played events to facts and dimension data model (Start Schema).

## Project Description
### Project: Data Pipelines with Airflow
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Datasets
Log data: s3://udacity-dend/log_data

Song data: s3://udacity-dend/song_data

## Building the operators
To complete the project, you need to build four different operators that will stage the data, transform the data, and run checks on data quality.

You can reuse the code from Project 2, but remember to utilize Airflow's built-in functionalities as connections and hooks as much as possible and let Airflow do all the heavy-lifting when it is possible.

All of the operators and task instances will run SQL statements against the Redshift database. However, using parameters wisely will allow you to build flexible, reusable, and configurable operators you can later apply to many kinds of data pipelines with Redshift and with other databases.

### Stage Operator
The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

### Fact and Dimension Operators
With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

### Data Quality Operator
The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

### Project setup
    Pre-requisites : 
        - Amazon Redshift cluster with accessible by awsuser. 
        - Once Airflow is up add airflow connection to AWS.
        - Connect to Amazon Reshift cluster and create all required tables from "/airflow/dag/create_tables.sql"

    Project Files :
        - airflow/dags/sparkify_workflow_etl_dag.py : The DAG defination python file for all the Operators. 
        - airflow/plugins/operators/stage_redshift.py : The PythonOperator to load any raw data (e.g here JSON) to Redshift tables.
        - ... load_fact.py : The PythonOperator to load from staging tables to target facts tables specified in the argument.
        - ... load_dimension.py : The PythonOperator to load from staging tables to dimension tables specified in the argument.

    To run the project just start Apache airflow - $sh /opt/airflow/start.sh
### Airflow admin screen
![](https://github.com/vinayms/data-pipeline-apache-airflow/img/Arflow_Udacity_Project_home_screen.png)

### DAG Graph View

![](https://github.com/vinayms/data-pipeline-apache-airflow/img/Airflow_Project_Pipeline_DAG_GraphView.png)

### DAG Tree View

![](https://github.com/vinayms/data-pipeline-apache-airflow/img/Airflow_Project_Pipeline_DAG_TreeView.png)
