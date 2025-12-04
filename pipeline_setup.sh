# Copy Hadoop configuration files (core-site.xml, hdfs-site.xml) to NiFi container for HDFS access
docker exec nifi mkdir -p /opt/nifi/nifi-current/conf/hadoop-conf
docker cp ./configs/nifi/core-site.xml nifi:/opt/nifi/nifi-current/conf/hadoop-conf/
docker cp ./configs/nifi/hdfs-site.xml nifi:/opt/nifi/nifi-current/conf/hadoop-conf/ 

# Verify the files are copied and the owner is nifi
docker exec nifi ls -la /opt/nifi/nifi-current/conf/hadoop-conf/ 

# Change ownership to nifi user
docker exec -u root nifi chown -R nifi:nifi /opt/nifi/nifi-current/conf/hadoop-conf/
docker exec -u root nifi chown -R nifi:nifi /opt/nifi/nifi-current/input
docker exec -u root nifi chmod -R 777 /opt/nifi/nifi-current/input

# Copy the required NAR files (these are the HDFS processors libraries)
docker cp ./nifi-extensions/nifi-hadoop-libraries-nar-2.0.0.nar nifi:/opt/nifi/nifi-current/lib/
docker cp ./nifi-extensions/nifi-hadoop-nar-2.0.0.nar nifi:/opt/nifi/nifi-current/lib/

# Restart NiFi to load the new configurations and libraries
docker-compose -f final-project-cluster-docker-compose.yaml restart nifi

# Create HDFS directory for NiFi to write weather and sales data
docker exec namenode hdfs dfs -mkdir -p /staging_zone/nifi/weather_sales_data
docker exec namenode hdfs dfs -chmod -R 777 /staging_zone/nifi/weather_sales_data

# Copy the updated Airflow DAG to the Airflow dags folder in the Namenode container
docker cp ./dags/final_project_dag.py namenode:/root/airflow/dags

# Copy the ETL script to the Airflow dags/scripts folder in the Namenode container
docker exec namenode mkdir -p /root/airflow/dags/scripts
docker cp ./dags/scripts/project_etl.py namenode:/root/airflow/dags/scripts