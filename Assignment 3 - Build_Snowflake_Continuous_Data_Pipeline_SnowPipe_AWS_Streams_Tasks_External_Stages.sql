/*GOALS
In this project, we to build Data PIPELINE to automate the manual steps involved in building 
and managing ELT logic for transforming and optimizing continuous data loads using 
Snowflake DATA PIPELINE. We will use the Snowflake features to enable continuous data pipelines.
➢ External Stage on s3
➢ SnowPipe
➢ Streams
➢ Tasks
➢ Stored Procedures*/

	USE ROLE ACCOUNTADMIN;

	USE WAREHOUSE COMPUTE_WH;

	CREATE OR REPLACE SCHEMA demo_db.person_schema;

/*External Stage on S3:
a.Create User in AWS with Programmatic access and copy the credentials.
b. Create s3 bucket 
c. Create Stage: Use below SQL statement in Snowflake to create external stage on s3(AWS).*/
	CREATE OR REPLACE STAGE demo_db.person_schema.per_ext_json_a3
	URL='s3://aws-snowpipe-streams-tasks-a3/'
	CREDENTIALS=(AWS_KEY_ID='*********************' 
	AWS_SECRET_KEY='**/*********************/*****'); 
	
	DESC STAGE per_ext_json_a3;

	LIST @demo_db.person_schema.per_ext_json_a3;

-- d. CREATE table in Snowflake with VARIANT column. 
	CREATE OR REPLACE TABLE person_nested (
	person_data VARIANT );

-- Create file format for JSON with strip_outer_array
	CREATE OR REPLACE file format json_a3_file_format
	type = 'JSON'
	compression = 'AUTO'
	STRIP_OUTER_ARRAY = TRUE;

-- e. Create a Snowpipe with Auto Ingest Enabled

	CREATE OR REPLACE PIPE person_data_pipe 
	auto_ingest = TRUE AS
	copy INTO person_nested
	FROM (SELECT $1 FROM @demo_db.person_schema.per_ext_json_a3)
    file_format=(format_name = demo_db.person_schema.json_a3_file_format)
    ON_ERROR = 'CONTINUE';

-- f. Subscribe the Snowflake SQS Queue in s3:
	DESC demo_db.person_schema.pipe person_data_pipe;
	
-- g. Test Snowpipe by copying the sample JSON file and upload the file to s3 in path
	ALTER PIPE demo_db.person_schema.person_data_pipe REFRESH;

/*Below are few ways we can validation if Snowpipe ran successfully. 
1. Check the pipe status using below command, it shows RUNNIG and it also shows pending FileCount.*/ 
	SELECT system$pipe_status ('person_data_pipe');
	
/*2. Check COPY_HISTORY for the table you are loading data to. If there is any error with Data Load, 
you can find that error here to debug the Load issue. 
3. Finally check if data is loaded to table by querying the table.*/

	SELECT * FROM person_nested;

	SELECT * FROM person_nested_stream;

/*Change Data Capture using Streams, Tasks and Merge. 
1.Create Streams on PERSON_NESTED table to capture the change data on PERSON_NESTED table and use 
TASKS to Run SQL/Stored Procedure to Unnested the data from PERSON_NESTED and create PERSON_MASTER table.*/
	CREATE OR REPLACE stream demo_db.person_schema.person_nested_stream 
	ON TABLE person_nested;
 
-- 2. Create a table to Load the unnested data from PERSON_NESTED.
	CREATE OR REPLACE TABLE demo_db.person_schema.PERSON_MASTER ( 
    Id NUMBER,
    Name STRING,
    Age NUMBER,
    Location STRING,
    ZIP STRING
	);

	CREATE OR REPLACE TABLE demo_db.person_schema.person_age (
		Name STRING,
		Age NUMBER
	);

	CREATE OR REPLACE TABLE demo_db.person_schema.person_location (
		Name STRING,
		Location STRING
	);

/*3. Create a TASK which run every 1 min and look for data in Stream PERSON_NESTED_STREAM, if data found 
in Stream then task will EXECUTE if not TASK will be SKIPPED without any doing anything.*/
	CREATE OR REPLACE TASK demo_db.person_schema.person_task_1
	WAREHOUSE='compute_wh' 
	SCHEDULE='1 minute'
	WHEN SYSTEM$STREAM_HAS_DATA('DEMO_DB.PERSON.PERSON_NESTED_STREAM')
	AS
	CALL demo_db.person_schema.proc_per_mas_data();
	
	DESC TASK demo_db.person_schema.person_task_1;
	
4. Test PIPELINE 
-- a) All the tables and Steam is empty, if not Truncate them. 
	TRUNCATE TABLE person_nested;
	TRUNCATE TABLE person_master;
	TRUNCATE TABLE person_age;
	TRUNCATE TABLE person_location;

/* b) Upload sample JSON data to s3 created 
c) Select data from PERSON_NESTED: Snowpipe would have loaded data to PERSON_NESTED table 
based on s3 sqs event notification. 
d) Check COPY HISTORY to know the status of COPY command and number of files copied. 
e) Steams capture any data change on the source table(PERSON_NESTED). So all the new data added 
to PERSON_NESTED should be in PERSON_NESTED_STREAM. Stream also contains additional columns which 
says if its INSERT/UPDATE/DELETE and it also contain unique METADATA$ROW_ID. Check those Columns. 
f) As we have created task to run every 1 min if there is data in Stream, you should be able to see 
the data in PERSON_MASTER table now. */
	ALTER TASK demo_db.person_schema.person_task_1 RESUME;
	
/*g) Once stream gets consumed in any DML operation the data from stream(PERSON_NESTED_STREAM) 
will be erased, PERSON_NESTED_STREAM steam will be empty
now as TASK ran and loaded the data to PERSON_MASTER. */
	SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY
	(TASK_NAME=>'demo_db.person_schema.person_task_1', RESULT_LIMIT=>10)); 

/* ELT IN SNOWFLAKE USING STORED PROCEDURE 
a) Create stored procedure to run Multiple SQL statements to automate data Load from 
PERSON_MASTER to two tables PERSON_AGE(Name, Age) and PERSON_LOCATION(Name, Location). 
This stored procedure should be called by TASK.
b) Stored Procedure Call :
c) CALL PERSON_MASTER_PROCEDURE(arguments1); Create Stored Procedure which runs below 2 SQLs.
	1. Insert data into Location table from Person Master table.
/* 	2. Insert data into Age table from Person Master table.*/ 

	CREATE OR REPLACE PROCEDURE  demo_db.person_schema.proc_per_mas_data()
	RETURNS STRING
	LANGUAGE JAVASCRIPT
	EXECUTE AS OWNER
	AS
	$$
	var sql_merge = `
		MERGE INTO demo_db.person_schema.person_master T
		USING (
			SELECT 
				S.PERSON_DATA:ID::INT AS ID,
				S.PERSON_DATA:Name::STRING AS NAME,
				S.PERSON_DATA:age::STRING AS AGE,
				S.PERSON_DATA:location::STRING AS LOCATION,
				S.PERSON_DATA:zip::STRING AS ZIP
			FROM demo_db.person_schema.person_nested_stream S
			WHERE S.METADATA$ACTION = 'INSERT' OR S.METADATA$ISUPDATE = 'TRUE'
		) S
		ON T.ID = S.ID
		WHEN MATCHED THEN
			UPDATE SET
				T.NAME = S.NAME,
				T.AGE = S.AGE,
				T.LOCATION = S.LOCATION,
				T.ZIP = S.ZIP
		WHEN NOT MATCHED THEN
			INSERT (ID, NAME, AGE, LOCATION, ZIP)
			VALUES (S.ID, S.NAME, S.AGE, S.LOCATION, S.ZIP);
	`;
	 
	var sql_insert_1 = `
		INSERT INTO demo_db.person_schema.person_age (Name, Age)
		SELECT NAME, AGE
		FROM demo_db.person_schema.person_master;
	`;
	 
	var sql_insert_2 = `
		INSERT INTO demo_db.person_schema.person_location (Name, LOCATION)
		SELECT NAME, LOCATION
		FROM demo_db.person_schema.person_master;
	`;
	 
	snowflake.execute({sqlText: sql_merge});
	 
	snowflake.execute({sqlText: sql_insert_1});
	 
	snowflake.execute({sqlText: sql_insert_2});

	RETURN `SUCCESS`;
	$$;

	select * from demo_db.person_schema.person_master;

	select * from demo_db.person_schema.person_location;

	select * from demo_db.person_schema.person_age;








 











