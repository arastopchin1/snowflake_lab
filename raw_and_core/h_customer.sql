-- Create table in RAW area
create or replace table epam_lab.raw.customer (
  c_custkey    INTEGER not null,
  c_name       VARCHAR(27),
  c_address    VARCHAR(42),
  c_nationkey  INTEGER,
  c_phone      CHAR(17),
  c_acctbal    VARCHAR(20),
  c_mktsegment CHAR(12),
  c_comment    VARCHAR(119)
);

-- Create table in CORE_DWH area
create or replace table epam_lab.core_dwh.customer (
  c_custkey    INTEGER not null,
  c_name       VARCHAR(25),
  c_address    VARCHAR(40),
  c_nationkey  INTEGER,
  c_phone      CHAR(15),
  c_acctbal    FLOAT8,
  c_mktsegment CHAR(10),
  c_comment    VARCHAR(117)
);

-- Create table stream on created RAW table to use it when load data to CORE_DWH
create or replace stream epam_lab.raw.stream_customer on table epam_lab.raw.customer;

-- Create task for extraction from stage
create or replace task epam_lab.raw.customer_data_from_stage
	warehouse = 'COMPUTE_WH'
    schedule = 'USING CRON 0 0 * * * UTC'
    as
copy into epam_lab.raw.customer
from @internal_lab_stage/h_customer.dsv
file_format = (format_name=epam_lab.raw.lab_dsv_format);

-- Suspend previous task since following one is dependent
alter task epam_lab.raw.customer_data_from_stage suspend;
alter task epam_lab.raw.customer_raw_to_core suspend;

-- Create task for moving data from RAW to CORE_DWN area
create or replace task epam_lab.raw.customer_raw_to_core
	warehouse = 'COMPUTE_WH'
    after epam_lab.raw.customer_data_from_stage
    as
insert into epam_lab.core_dwh.customer(c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
select c_custkey as c_custkey
, TRIM(c_name, '"') as c_name
, TRIM(c_address, '"') as c_address
, c_nationkey as c_nationkey
, TRIM(c_phone, '" ') as c_phone
, REPLACE(c_acctbal, ',', '.')::float as c_acctbal
, TRIM(c_mktsegment, '" ') as c_mktsegment
, TRIM(c_comment, '" .') as c_comment
from epam_lab.raw.stream_customer
where metadata$action = 'INSERT';

-- Resume previous task
alter task epam_lab.raw.customer_raw_to_core resume;
alter task epam_lab.raw.customer_data_from_stage resume;

-- Execute task which will trigger the following task after completion
execute task epam_lab.raw.customer_data_from_stage;

-- Check that we have data in RAW area
select * from epam_lab.raw.customer limit 100;

-- Check that we have data in CORE_DWH area
select * from epam_lab.core_dwh.customer limit 100;
