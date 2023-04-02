-- Create table in RAW area
create or replace table epam_lab.raw.h_nation_raw (
  n_nationkey INTEGER not null,
  n_name      CHAR(27),
  n_regionkey INTEGER,
  n_comment   VARCHAR(155)
);

-- Create table in CORE_DWH area
create or replace table epam_lab.core_dwh.h_nation
(
  n_nationkey INTEGER not null,
  n_name      CHAR(27),
  n_regionkey INTEGER,
  n_comment   VARCHAR(155)
);

-- Create table stream on created RAW table to use it when load data to CORE_DWH
create or replace stream stream_nation_raw on table epam_lab.raw.h_nation_raw;

-- Create task for extraction from stage
create or replace task epam_lab.raw.nation_data_from_stage
	warehouse = 'COMPUTE_WH'
    schedule = 'USING CRON 0 0 * * * UTC'
    as
copy into epam_lab.raw.h_nation_raw
from @internal_lab_stage/h_nation.dsv
file_format = (format_name=epam_lab.raw.lab_dsv_format);

-- Suspend previous task since following one is dependent
alter task epam_lab.raw.nation_data_from_stage suspend;
alter task epam_lab.raw.nation_raw_to_core suspend;

-- Create task for moving data from RAW to CORE_DWN area
create or replace task epam_lab.raw.nation_raw_to_core
	warehouse = 'COMPUTE_WH'
    after epam_lab.raw.nation_data_from_stage
    as
insert into epam_lab.core_dwh.h_nation(n_nationkey, n_name, n_regionkey, n_comment)
select n_nationkey
, TRIM(n_name, '" ') as n_name
, n_regionkey
, TRIM(n_comment, '" .') as n_comment
from epam_lab.raw.stream_nation_raw
where metadata$action = 'INSERT';

-- Resume previous task
alter task epam_lab.raw.nation_raw_to_core resume;
alter task epam_lab.raw.nation_data_from_stage resume;

-- Execute task which will trigger the following task after completion
execute task nation_data_from_stage;

-- Check that we have data in RAW area
select * from epam_lab.raw.h_nation_raw;

-- Check that we have data in CORE_DWH area
select * from epam_lab.core_dwh.h_nation;
