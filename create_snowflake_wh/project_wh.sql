CREATE WAREHOUSE IF NOT EXISTS sales_weather_wh
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE DATABASE sales_weather_db;

USE sales_weather_db;

CREATE OR REPLACE SCHEMA gold;

CREATE OR REPLACE TABLE gold.DIM_DATE (
    date_id         INTEGER,
    full_date       DATE,
    year            INTEGER,
    month           INTEGER,
    day             INTEGER,
    day_name        VARCHAR(3),
    is_weekend      INTEGER
);

CREATE OR REPLACE TABLE gold.DIM_PRODUCT (
    product_id          VARCHAR(20),
    product_name        VARCHAR(255),
    category_name       VARCHAR(100),
    unit                VARCHAR(20),
    price_unit_gbp      DECIMAL(10,2)    
);

CREATE OR REPLACE TABLE gold.DIM_SUPERMARKET (
    supermarket_id      VARCHAR(20),
    supermarket_name    VARCHAR(100) 
);

CREATE OR REPLACE TABLE gold.DIM_STATION (
    station_id      VARCHAR(50),
    latitude        DECIMAL(10,7),
    longitude       DECIMAL(10,7),
    city            VARCHAR(100)  
);

CREATE OR REPLACE TABLE gold.FACT_SALES (
    transc_id           VARCHAR(50),
    date_id             INTEGER,
    product_id          VARCHAR(20),
    supermarket_id      VARCHAR(20),
    station_id          VARCHAR(50),
    quantity            INTEGER,
    price_gbp           DECIMAL(10,2),
    price_unit_gbp      DECIMAL(10,3),
    avg_temp            DECIMAL(5,2),
    max_temp            DECIMAL(5,2),
    min_temp            DECIMAL(5,2),
    precipitation       DECIMAL(6,2),
    temp_category       VARCHAR(20),
    rain_category       VARCHAR(20)
);



CREATE USER spark_user PASSWORD = 'spark_password'
LOGIN_NAME = 'spark_user'
DEFAULT_ROLE = ACCOUNTADMIN
MUST_CHANGE_PASSWORD = FALSE;

GRANT USAGE ON DATABASE sales_weather_db TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA sales_weather_db.gold TO ROLE ACCOUNTADMIN;

GRANT USAGE ON WAREHOUSE sales_weather_wh TO ROLE ACCOUNTADMIN;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA sales_weather_db.gold TO ROLE ACCOUNTADMIN;

SHOW GRANTS TO USER spark_user;
GRANT ROLE ACCOUNTADMIN TO USER spark_user;

SHOW GRANTS TO ROLE ACCOUNTADMIN;