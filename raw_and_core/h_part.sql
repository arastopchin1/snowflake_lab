-- Create table in RAW area
create or replace table epam_lab.raw.part (
  p_partkey     INTEGER not null,
  p_name        VARCHAR(57),
  p_mfgr        CHAR(27),
  p_brand       CHAR(12),
  p_type        VARCHAR(27),
  p_size        INTEGER,
  p_container   CHAR(12),
  p_retailprice INTEGER,
  p_comment     VARCHAR(25)
);

-- Create table in CORE_DWH area
create or replace table epam_lab.core_dwh.part (
  p_partkey     INTEGER not null,
  p_name        VARCHAR(55),
  p_mfgr        CHAR(25),
  p_brand       CHAR(10),
  p_type        VARCHAR(25),
  p_size        INTEGER,
  p_container   CHAR(10),
  p_retailprice INTEGER,
  p_comment     VARCHAR(23)
);

-- Create table stream on created RAW table to use it when load data to CORE_DWH
create or replace stream epam_lab.raw.stream_part on table epam_lab.raw.part;

-- Create task for extraction from stage
create or replace task epam_lab.raw.part_data_from_stage
	warehouse = 'COMPUTE_WH'
    schedule = 'USING CRON 0 0 * * * UTC'
    as
copy into epam_lab.raw.part
from @internal_lab_stage/h_part.dsv
file_format = (format_name=epam_lab.raw.lab_dsv_format);

-- Suspend previous task since following one is dependent
alter task epam_lab.raw.part_data_from_stage suspend;
alter task epam_lab.raw.part_raw_to_core suspend;

-- Create task for moving data from RAW to CORE_DWN area
create or replace task epam_lab.raw.part_raw_to_core
	warehouse = 'COMPUTE_WH'
    after epam_lab.raw.part_data_from_stage
    as
insert into epam_lab.core_dwh.part(p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
select p_partkey as p_partkey
, TRIM(p_name, '"') as p_name
, TRIM(p_mfgr, '" ') as p_mfgr
, TRIM(p_brand, '" ') as p_brand
, TRIM(p_type, '"') as p_type
, p_size as p_size
, TRIM(p_container, '" ') as p_container
, p_retailprice as p_retailprice
, TRIM(p_comment, '" .') as p_comment
from epam_lab.raw.stream_part
where metadata$action = 'INSERT';

-- Resume previous task
alter task epam_lab.raw.part_raw_to_core resume;
alter task epam_lab.raw.part_data_from_stage resume;

-- Execute task which will trigger the following task after completion
execute task epam_lab.raw.part_data_from_stage;

-- Check that we have data in RAW area
select * from epam_lab.raw.part limit 100;

-- Check that we have data in CORE_DWH area
select * from epam_lab.core_dwh.part limit 100;
