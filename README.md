# Weather-Driven Sales Optimization System

## Description
End-to-End Big Data Pipeline • NiFi • HDFS • PySpark • Airflow • Snowflake • Dashboard

This project delivers a complete big-data analytics platform that integrates large-scale UK retail sales data with NOAA global weather observations to uncover how temperature, rainfall, and climate conditions influence product demand across cities and supermarkets.

The system is built using a full big-data stack, orchestrated and deployed inside a Docker-based cluster.

## Table of Contents
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup](#setup)

### Features
- Ingestion using Apache NiFi.
- Raw data stored in HDFS.
- Distributed transformations with Spark.
- Star-schema dimensional modeling.
- Cloud warehouse loading into Snowflake.
- Full orchestration using Airflow DAG.
- Dashboard for insights.
- Geospatial mapping (Haversine distance) between weather stations & cities.

### Architecture

  <img width="1212" height="590" alt="image" src="https://github.com/user-attachments/assets/2241cda7-142e-4f3e-a350-b3c2c9bc6055" />

### Prerequisites
To run this project, ensure you have the following:
- Docker and Docker Compose.
- Snowflake cloud Account.

### Setup
1. Clone the Repo
   ```bash
   git clone https://github.com/Hager-Ibraheem/Weather_Driven_Sales_Optimization_System.git
   cd Weather_Driven_Sales_Optimization_System
   ```
2. Start the Docker Big-Data Cluster
   ```bash
   sudo docker-compose -f final-project-cluster-docker-compose.yaml up -d
   ```
3. Run pipeline_setup.sh
   ```bash
   chmod +x pipeline_setup.sh
   ./pipeline_setup.sh
   ```
4. Open NiFi UI at https://localhost:8443/nifi (username 'admin', password 'ctsBtRBKHRAx69EqUghvvgEvjnaLjFEB') 
   - Create new process group
     <img width="596" height="335" alt="Screenshot 2025-12-04 205712" src="https://github.com/user-attachments/assets/467837c3-9b04-4676-b335-c9e9f3b8186a" />
   - In process group, Drag processors onto the canvas in this order:
        * GetFile - to read files from local directory.
        * PutHDFS - to write to HDFS.
   - Configure GetFile Processor:     
        * Input Directory: /opt/nifi/nifi-current/input
        * Keep Source File: false
        * File Filter: .*\\.(csv|parquet|txt) 
        * Minimum File Age: 0 sec
        * Polling Interval: 10 sec
   - Configure PutHDFS Processor:        
        * Hadoop Configuration Resources: /opt/nifi/nifi-current/conf/hadoop-conf/core-site.xml,/opt/nifi/nifi-current/conf/hadoop-conf/hdfs-site.xml
        * Directory: /staging_zone/nifi/weather_sales_data (HDFS destination path)
        * Conflict Resolution Strategy: replace
        * Compression codec: NONE
   - Make processors running:
     <img width="660" height="676" alt="Screenshot 2025-11-24 000350" src="https://github.com/user-attachments/assets/d4b05efc-e131-4ead-ab1e-5d4e0bfb01dc" />

5. Configure Snowflake
   - use 'project_wh.sql' file in 'create_snowflake_wh' folder to Create:
      * Warehouse
      * Database
      * Schema
      * Snowflake user
      * Grant permissions
   - change Snowflake connection parameters in 'project_etl.py'
    <img width="563" height="208" alt="image" src="https://github.com/user-attachments/assets/701a2eee-c2ff-4fde-bb0c-c100b54fc52c" />
    
   - Copy the ETL script to the Airflow dags/scripts folder in the Namenode container
     ```bash
     docker cp ./dags/scripts/project_etl.py namenode:/root/airflow/dags/scripts
     ```
6. Run Airflow DAG
   - Open Airflow UI:
    http://localhost:3000 (username 'admin', password 'admin') 
    
    - Enable and trigger DAG: final_project_workflow
