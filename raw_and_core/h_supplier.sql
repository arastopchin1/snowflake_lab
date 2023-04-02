-- Create table in RAW area
create or replace table epam_lab.raw.supplier (
  s_suppkey   INTEGER not null,
  s_name      CHAR(50),
  s_address   VARCHAR(100),
  s_nationkey INTEGER,
  s_phone     CHAR(50),
  s_acctbal   VARCHAR(50),
  s_comment   VARCHAR(200)
);

-- Create table in CORE_DWH area
create or replace table epam_lab.core_dwh.supplier
(
  s_suppkey   INTEGER not null,
  s_name      CHAR(25),
  s_address   VARCHAR(40),
  s_nationkey INTEGER,
  s_phone     CHAR(15),
  s_acctbal   FLOAT8,
  s_comment   VARCHAR(101)
);

-- Create table stream on created RAW table to use it when load data to CORE_DWH
create or replace stream epam_lab.raw.stream_supplier on table epam_lab.raw.supplier;

-- Create task for extraction from stage
create or replace task epam_lab.raw.supplier_data_from_stage
	warehouse = 'COMPUTE_WH'
    schedule = 'USING CRON 0 0 * * * UTC'
    as
copy into epam_lab.raw.supplier
from @internal_lab_stage/h_supplier.dsv
file_format = (format_name=epam_lab.raw.lab_dsv_format);

-- Suspend previous task since following one is dependent
alter task epam_lab.raw.supplier_data_from_stage suspend;
alter task epam_lab.raw.supplier_raw_to_core suspend;

-- Create task for moving data from RAW to CORE_DWN area
create or replace task epam_lab.raw.supplier_raw_to_core
	warehouse = 'COMPUTE_WH'
    after epam_lab.raw.supplier_data_from_stage
    as
insert into epam_lab.core_dwh.supplier(s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
select s_suppkey
, TRIM(s_name, '" ') as s_name
, TRIM(s_address, '" ') as s_address
, s_nationkey
, TRIM(s_phone, '" ') as s_phone
, REPLACE(s_acctbal, ',', '.')::float as s_acctbal
, TRIM(s_comment, '" .') as s_comment
from epam_lab.raw.stream_supplier
where metadata$action = 'INSERT';

-- Resume previous task
alter task epam_lab.raw.supplier_raw_to_core resume;
alter task epam_lab.raw.supplier_data_from_stage resume;

-- Execute task which will trigger the following task after completion
execute task epam_lab.raw.supplier_data_from_stage;

-- Check that we have data in RAW area
select * from epam_lab.raw.supplier;

-- Check that we have data in CORE_DWH area
select * from epam_lab.core_dwh.supplier;
