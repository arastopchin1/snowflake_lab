-- Create table in RAW area
create or replace table epam_lab.raw.partsupp (
  ps_partkey    INTEGER not null,
  ps_suppkey    INTEGER not null,
  ps_availqty   INTEGER,
  ps_supplycost VARCHAR(10) not null,
  ps_comment    VARCHAR(201)
);

-- Create table in CORE_DWH area
create or replace table epam_lab.core_dwh.partsupp (
  ps_partkey    INTEGER not null,
  ps_suppkey    INTEGER not null,
  ps_availqty   INTEGER,
  ps_supplycost FLOAT8 not null,
  ps_comment    VARCHAR(199)
);

-- Create table stream on created RAW table to use it when load data to CORE_DWH
create or replace stream epam_lab.raw.stream_partsupp on table epam_lab.raw.partsupp;

-- Create task for extraction from stage
create or replace task epam_lab.raw.partsupp_data_from_stage
	warehouse = 'COMPUTE_WH'
    schedule = 'USING CRON 0 0 * * * UTC'
    as
copy into epam_lab.raw.partsupp
from @internal_lab_stage/h_partsupp.dsv
file_format = (format_name=epam_lab.raw.lab_dsv_format);

-- Suspend previous task since following one is dependent
alter task epam_lab.raw.partsupp_data_from_stage suspend;
alter task epam_lab.raw.partsupp_raw_to_core suspend;

-- Create task for moving data from RAW to CORE_DWN area
create or replace task epam_lab.raw.partsupp_raw_to_core
	warehouse = 'COMPUTE_WH'
    after epam_lab.raw.partsupp_data_from_stage
    as
insert into epam_lab.core_dwh.partsupp(ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
select ps_partkey as ps_partkey
, ps_suppkey as ps_suppkey
, ps_availqty as ps_availqty
, REPLACE(ps_supplycost, ',', '.')::float as ps_supplycost
, TRIM(ps_comment, '" .') as ps_comment
from epam_lab.raw.stream_partsupp
where metadata$action = 'INSERT';

-- Resume previous task
alter task epam_lab.raw.partsupp_raw_to_core resume;
alter task epam_lab.raw.partsupp_data_from_stage resume;

-- Execute task which will trigger the following task after completion
execute task epam_lab.raw.partsupp_data_from_stage;

-- Check that we have data in RAW area
select * from epam_lab.raw.partsupp limit 100;

-- Check that we have data in CORE_DWH area
select * from epam_lab.core_dwh.partsupp limit 100;
