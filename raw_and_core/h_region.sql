-- Create table in RAW area
create or replace table epam_lab.raw.h_region_raw (
  r_regionkey INTEGER,
  r_name      CHAR(50),
  r_comment   VARCHAR(152)
);

-- Create table in CORE_DWH area
create table epam_lab.core_dwh.h_region
(
  r_regionkey INTEGER,
  r_name      CHAR(25),
  r_comment   VARCHAR(152)
);

-- Create table stream on created RAW table to use it when load data to CORE_DWH
create or replace stream stream_region_raw on table epam_lab.raw.h_region_raw;



-- Create task for extraction from stage
create or replace task epam_lab.raw.region_data_from_stage
	warehouse = 'COMPUTE_WH'
    schedule = 'USING CRON 0 0 * * * UTC'
    as
copy into epam_lab.raw.h_region_raw
from @internal_lab_stage/h_region.csv
file_format = (format_name=epam_lab.raw.lab_csv_format);


-- Suspend previous task since following one is dependent
alter task epam_lab.raw.region_data_from_stage suspend;
alter task epam_lab.raw.region_raw_to_core suspend;

-- Create task for moving data from RAW to CORE_DWN area using table stream
create or replace task epam_lab.raw.region_raw_to_core
	warehouse = 'COMPUTE_WH'
    after epam_lab.raw.region_data_from_stage
    as
insert into epam_lab.core_dwh.h_region(r_regionkey, r_name, r_comment)
select r_regionkey
, TRIM(r_name, '" ') as r_name
, TRIM(r_comment, '" .') as r_comment
from epam_lab.raw.stream_region_raw
where metadata$action = 'INSERT';

-- Resume previous task
alter task epam_lab.raw.region_data_from_stage resume;
alter task epam_lab.raw.region_raw_to_core resume;

-- Execute task which will trigger the following task after completion
execute task region_data_from_stage;

-- Check that we have data in RAW area
select * from epam_lab.raw.h_region_raw;

-- Check that we have data in CORE_DWH area
select * from epam_lab.core_dwh.h_region;
