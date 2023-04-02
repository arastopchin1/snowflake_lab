-- Create table in RAW area
create or replace table epam_lab.raw.orders (
  o_orderkey      INTEGER not null,
  o_custkey       INTEGER not null,
  o_orderstatus   CHAR(3),
  o_totalprice    VARCHAR(20),
  o_orderdate     VARCHAR(20),
  o_orderpriority CHAR(17),
  o_clerk         CHAR(17),
  o_shippriority  INTEGER,
  o_comment       VARCHAR(81)
);

-- Create table in CORE_DWH area
create or replace table epam_lab.core_dwh.orders
(
  o_orderkey      INTEGER not null,
  o_custkey       INTEGER not null,
  o_orderstatus   CHAR(1),
  o_totalprice    FLOAT8,
  o_orderdate     DATE,
  o_orderpriority CHAR(15),
  o_clerk         CHAR(15),
  o_shippriority  INTEGER,
  o_comment       VARCHAR(79)
);

-- Create table stream on created RAW table to use it when load data to CORE_DWH
create or replace stream epam_lab.raw.stream_orders on table epam_lab.raw.orders;

-- Create task for extraction from stage
create or replace task epam_lab.raw.orders_data_from_stage
	warehouse = 'COMPUTE_WH'
    schedule = 'USING CRON 0 0 * * * UTC'
    as
copy into epam_lab.raw.orders
from @internal_lab_stage/h_order.dsv
file_format = (format_name=epam_lab.raw.lab_dsv_format);

-- Suspend previous task since following one is dependent
alter task epam_lab.raw.orders_data_from_stage suspend;
alter task epam_lab.raw.orders_raw_to_core suspend;

-- Create task for moving data from RAW to CORE_DWN area
create or replace task epam_lab.raw.orders_raw_to_core
	warehouse = 'COMPUTE_WH'
    after epam_lab.raw.orders_data_from_stage
    as
insert into epam_lab.core_dwh.orders(o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
select o_orderkey
, o_custkey
, TRIM(o_orderstatus, '"') as o_orderstatus
, REPLACE(o_totalprice, ',', '.')::float as o_totalprice
, TO_DATE(o_orderdate, 'DD.MM.YY') as o_orderdate
, TRIM(o_orderpriority, '" ') as o_orderpriority
, TRIM(o_clerk, '" ') as o_clerk
, o_shippriority
, TRIM(o_comment, '" .') as o_comment
from epam_lab.raw.stream_orders
where metadata$action = 'INSERT';

-- Resume previous task
alter task epam_lab.raw.orders_raw_to_core resume;
alter task epam_lab.raw.orders_data_from_stage resume;

-- Execute task which will trigger the following task after completion
execute task epam_lab.raw.orders_data_from_stage;

-- Check that we have data in RAW area
select * from epam_lab.raw.orders limit 100;

-- Check that we have data in CORE_DWH area
select * from epam_lab.core_dwh.orders limit 100;
