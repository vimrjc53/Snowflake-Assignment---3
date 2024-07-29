/*GOALS
In this project, we to build Data PIPELINE to automate the manual steps involved in building 
and managing ELT logic for transforming and optimizing continuous data loads using 
Snowflake DATA PIPELINE. We will use the Snowflake features to enable continuous data pipelines.
➢ External Stage on s3
➢ SnowPipe
➢ Streams
➢ Tasks
➢ Stored Procedures


External Stage on S3:
a.Create User in AWS with Programmatic access and copy the credentials.
b. Create s3 bucket 
c. Create Stage: Use below SQL statement in Snowflake to create external stage on s3(AWS). 
d. CREATE table in Snowflake with VARIANT column. 
e. Create a Snowpipe with Auto Ingest Enabled
f. Subscribe the Snowflake SQS Queue in s3: 
g. Test Snowpipe by copying the sample JSON file and upload the file to s3 in path

Below are few ways we can validation if Snowpipe ran successfully. 
1 . Check the pipe status using below command, it shows RUNNIG and it also shows pendingFileCount. 
2. Check COPY_HISTORY for the table you are loading data to. If there is any error with Data Load, 
you can find that error here to debug the Load issue. 
3. Finally check if data is loaded to table by querying the table. 

Change Data Capture using Streams, Tasks and Merge. 
1.Create Streams on PERSON_NESTED table to capture the change data on PERSON_NESTED table and use 
TASKS to Run SQL/Stored Procedure to Unnested the data from PERSON_NESTED and create PERSON_MASTER table. 
2. Create a table to Load the unnested data from PERSON_NESTED.
3. Create a TASK which run every 1 min and look for data in Stream PERSON_NESTED_STREAM, if data found 
in Stream then task will EXECUTE if not TASK will be SKIPPED without any doing anything. 

4. Test PIPELINE 
a) All the tables and Steam is empty, if not Truncate them. 
b) Upload sample JSON data to s3 created 
c) Select data from PERSON_NESTED: Snowpipe would have loaded data to PERSON_NESTED table 
based on s3 sqs event notification. 
d) Check COPY HISTORY to know the status of COPY command and number of files copied. 
e) Steams capture any data change on the source table(PERSON_NESTED). So all the new data added 
to PERSON_NESTED should be in PERSON_NESTED_STREAM. Stream also contains additional columns which says if 
its INSERT/UPDATE/DELETE and it also contain unique METADATA$ROW_ID. Check those Columns. 
f) As we have created task to run every 1 min if there is data in Stream, you should be able to see the data in PERSON_MASTER table now. 
g) Once stream gets consumed in any DML operation the data from stream(PERSON_NESTED_STREAM) will be erased, PERSON_NESTED_STREAM steam will be empty
now as TASK ran and loaded the data to PERSON_MASTER. ELT IN SNOWFLAKE USING STORED PROCEDURE a) Create stored procedure to run Multiple SQL statements to automate data Load from PERSON_MASTER to two tables PERSON_AGE(Name, Age) and PERSON_LOCATION(Name, Location). This stored procedure should be called by TASK.
b) Stored Procedure Call :
c) CALL PERSON_MASTER_PROCEDURE(arguments1); Create Stored Procedure which runs below 2 SQLs.
1. Insert data into Location table from Person Master table.
2. Insert data into Age table from Person Master table.*/


USE ROLE ACCOUNTADMIN;

USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE SCHEMA demo_db.person_schema;

TRUNCATE TABLE person_nested;

-- Create User in AWS with Programmatic access and creat external stage using Access Key and AWS Key ID
CREATE OR REPLACE stage demo_db.person_schema.per_ext_json_a3
URL='s3://aws-snowpipe-streams-tasks-a3/'
CREDENTIALS=(AWS_KEY_ID='*******************' 
AWS_SECRET_KEY='***************************************************************');

DESC STAGE per_ext_json_a3;

LIST @demo_db.person_schema.per_ext_json_a3;

-- Create person_nested table in Snowflake with VARIANT column.
CREATE OR REPLACE TABLE person_nested (
    person_data VARIANT );
 
CREATE OR REPLACE file format json_a3_file_format
type = 'JSON'
compression = 'AUTO'
STRIP_OUTER_ARRAY = TRUE;
 
-- Create a Snowpipe with Auto Ingest Enabled
CREATE OR REPLACE pipe person_data_pipe 
auto_ingest = TRUE AS
copy INTO person_nested
  FROM (
  SELECT
 $1
    FROM @demo_db.person_schema.per_ext_json_a3)
      file_format=(format_name = demo_db.person_schema.json_a3_file_format)
      ON_ERROR = 'CONTINUE';

-- Subscribe the Snowflake, provide snowpipe ARN in S3 SQS Queue.
DESC pipe person_data_pipe;

-- Validate
SELECT * FROM TABLE (validate_pipe_load
 (
  pipe_name=>'DEMO_DB.PERSON_SCHEMA.PERSON_NESTED',
  start_time=>dateadd(hour,-1, current_timestamp())));

SELECT * FROM TABLE (information_schema.
copy_history(table_name=>'PERSON_NESTED',
start_time=>dateadd(hours,-1, current_timestamp())));

-- Test Snowpipe by copying the sample JSON file and upload the file to s3 in path
ALTER pipe person_data_pipe refresh;

SELECT system$pipe_status ('person_data_pipe');

SELECT * FROM person_nested;

   
-- Create a stream object
CREATE OR REPLACE stream demo_db.person_schema.person_nested_stream 
ON TABLE person_nested;

-------------

select * from person_nested;

SELECT * FROM person_nested_stream;

SELECT * FROM PERSON_MASTER;

CREATE OR REPLACE TASK demo_db.person_schema.person_task_1
WAREHOUSE='compute_wh' 
SCHEDULE='1 minute'
WHEN SYSTEM$STREAM_HAS_DATA('DEMO_DB.PERSON.PERSON_NESTED_STREAM')
AS
CALL DEMO_DB.PERSON_SCHEMA.PROC_PERSON_DATA();


DESC TASK demo_db.person_schema.person_task_1;

desc table person_master;

alter task demo_db.person_schema.person_task_1 resume;

SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY
(TASK_NAME=>'demo_db.person_schema.person_task_1', RESULT_LIMIT=>10));

CREATE TABLE demo_db.person_schema.PERSON_MASTER_1 ( 
    ID NUMBER,
        Name STRING,
        age NUMBER,
        location STRING,
         DECODE(zip STRING, '00000', zip STRING) AS zip
);
       
 


CREATE TABLE demo_db.person_schema.person_location_1 (
    ID NUMBER,
    location STRING,
    DECODE(person_data:zip::STRING, '', '00000', person_data:zip::STRING) AS zip
);
 


 -- Create procedure to create master_person table and merge data
CREATE OR REPLACE PROCEDURE DEMO_DB.PERSON_SCHEMA.PROC_PERSON_DATA()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    -- Merge data from PERSON_NESTED into PERSON_MASTER
    EXECUTE IMMEDIATE '
        MERGE INTO DEMO_DB.PERSON_SCHEMA.PERSON_MASTER AS target
        USING (
            SELECT DISTINCT 
                person_data:ID::NUMBER AS ID,
                person_data:Name::STRING AS Name, 
                person_data:age::NUMBER AS age, 
                person_data:location::STRING AS location, 
                IFF(person_data:zip::STRING = '''' OR person_data:zip::STRING IS NULL, ''00000'', person_data:zip::STRING) AS zip
            FROM DEMO_DB.PERSON_SCHEMA.person_nested
        ) AS source
        ON target.ID = source.ID
        WHEN MATCHED AND (source.METADATA$ACTION = ''DELETE'') AND (source.METADATA$ISUPDATE = ''FALSE'') THEN DELETE
        WHEN MATCHED AND (source.METADATA$ACTION = ''INSERT'') AND (source.METADATA$ISUPDATE = ''TRUE'') THEN
            UPDATE SET
                target.Name = source.Name,
                target.age = source.age,
                target.location = source.location,
                target.zip = source.zip,
        WHEN NOT MATCHED AND (source.METADATA$ACTION = ''INSERT'') THEN
            INSERT (ID, NAME, AGE, LOCATION, ZIP)
            VALUES (source.ID, source.Name, source.age, source.location, source.zip)
    ';
 
    -- -- Insert data into PERSON_AGE table
    -- EXECUTE IMMEDIATE '
    --     INSERT INTO DEMO_DB.PERSON_SCHEMA.PERSON_AGE (Name, Age)
    --     SELECT NAME, AGE
    --     FROM DEMO_DB.PERSON_SCHEMA.PERSON_MASTER;
    -- ';



    -- -- Insert data into PERSON_LOCATION table
    -- EXECUTE IMMEDIATE '
    --     INSERT INTO DEMO_DB.PERSON_SCHEMA.PERSON_LOCATION (Name, LOCATION)
    --     SELECT NAME, LOCATION
    --     FROM DEMO_DB.PERSON_SCHEMA.PERSON_MASTER
    --     WHERE LOCATION IS NOT NULL;
    -- ';
 
    RETURN 'Executed sccessfully';
END;
$$

call DEMO_DB.PERSON_SCHEMA.PROC_PERSON_DATA();


 
select * from demo_db.person_schema.person_master;

select * from demo_db.person_schema.person_location;

select * from demo_db.person_schema.person_age;










