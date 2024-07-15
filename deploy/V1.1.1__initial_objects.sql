// 1.CREATE DATABSE SCHEMAS IF NOT EXISTS

CREATE DATABASE IF NOT EXISTS MYDB;
CREATE SCHEMA IF NOT EXISTS MYDB.FILE_FORMATS;
CREATE SCHEMA IF NOT EXISTS MYDB.EXTERNAL_STAGES;

//2. CREATE STORAGE INTEGRATION OBJECT
CREATE OR REPLACE STORAGE INTEGRATION S3_INT
TYPE= EXTERNAL_STAGE
STORAGE_PROVIDER=S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN='arn:aws:iam::992382636540:role/aws-s3-snowflake-intg'
STORAGE_ALLOWED_LOCATIONS=('s3://snowpipedataloadriya1/pipes/')
COMMENT='INTEGRATION WITH AWS S3 BUCKETS';

desc storage integration s3_int;

//3. create a file format
create or replace file format mydb.file_formats.csv_fileformat
type=csv
field_delimiter=','
skip_header=1
empty_field_as_null=true;

//4.create stage object using storage integration
create or replace stage mydb.external_stages.stage_aws_pipes
url='s3://snowpipedataloadriya1/pipes/csv'
storage_integration=s3_int
file_format=mydb.file_formats.csv_fileformat;

//5.list the files in stages
list @mydb.external_stages.stage_aws_pipes;

//6.create a table 

create or replace table mydb.public.emp_data(id int,
first_name string,
last_name string,
email string, 
location string,
department string);

//7. create a schema for pipe related work
create or replace schema mydb.pipes;

//8. create a pipe
create or replace pipe mydb.pipes.employee_pipe
auto_ingest=true
as 
copy into mydb.public.emp_data
from @mydb.external_stages.stage_aws_pipes
pattern='.*employee.*';

//9. describe pipe

desc pipe mydb.pipes.employee_pipe;

//10. get notification channel arn and update the same to s3 bucket event notification menu ,sqs queue

//11. now upload the data into bucket in s3, and check data in this table after one minutes

select * from mydb.public.emp_data;

//12. steps for snowpipe troubleshooting
--check status of pipe & lastreiceivedmessagetimestamp , lastforwarededmessagetimestamp

select system$pipe_status('employee_pipe');
//13.check copy history
select * from table(information_schema.copy_history
(table_name => 'mydb.public.emp_data',
start_time=>dateadd(hour,-24,current_timestamp())));

//14. validate the data files
select * from table(information_schema.validate_pipe_load
(pipe_name=> 'mydb.pipes.employee_pipe',
start_time=> dateadd(hour,-24,current_timestamp()))
);

copy into mydb.public.emp_data
from @mydb.external_stages.stage_aws_pipes
files=('/sp_employee_5.csv');

select * from mydb.public.emp_data;

//15. when not used ,pause the pipe, when updating pipe, pause the pipe

alter pipe mydb.pipes.employee_pipe set pipe_execution_paused=true;

select system$pipe_status('employee_pipe');


show pipes;

desc pipe mydb.pipes.employee_pipe;

show pipes like '%employee%';

show pipes in database mydb;

show pipes in schema mydb.pipes;

show pipes like '%employee%' in database mydb;

SELECT CURRENT_REGION();