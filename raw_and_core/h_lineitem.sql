-- Create table in RAW area
create or replace table epam_lab.raw.lineitem (
  l_orderkey      INTEGER not null,
  l_partkey       INTEGER not null,
  l_suppkey       INTEGER not null,
  l_linenumber    INTEGER not null,
  l_quantity      INTEGER not null,
  l_extendedprice VARCHAR(50) not null,
  l_discount      VARCHAR(50) not null,
  l_tax           VARCHAR(50) not null,
  l_returnflag    CHAR(3),
  l_linestatus    CHAR(3),
  l_shipdate      VARCHAR(10),
  l_commitdate    VARCHAR(10),
  l_receiptdate   VARCHAR(10),
  l_shipinstruct  CHAR(27),
  l_shipmode      CHAR(12),
  l_comment       VARCHAR(46)
);

-- Create table in CORE_DWH area
create or replace table epam_lab.core_dwh.lineitem (
  l_orderkey      INTEGER not null,
  l_partkey       INTEGER not null,
  l_suppkey       INTEGER not null,
  l_linenumber    INTEGER not null,
  l_quantity      INTEGER not null,
  l_extendedprice FLOAT8 not null,
  l_discount      FLOAT8 not null,
  l_tax           FLOAT8 not null,
  l_returnflag    CHAR(1),
  l_linestatus    CHAR(1),
  l_shipdate      DATE,
  l_commitdate    DATE,
  l_receiptdate   DATE,
  l_shipinstruct  CHAR(25),
  l_shipmode      CHAR(10),
  l_comment       VARCHAR(44)
);

-- Create table stream on created RAW table to use it when load data to CORE_DWH
create or replace stream epam_lab.raw.stream_lineitem on table epam_lab.raw.lineitem;

-- Create task for extraction from stage
create or replace task epam_lab.raw.lineitem_data_from_stage
	warehouse = 'COMPUTE_WH'
    schedule = 'USING CRON 0 0 * * * UTC'
    as
copy into epam_lab.raw.lineitem
from @internal_lab_stage/h_lineitem.dsv
file_format = (format_name=epam_lab.raw.lab_dsv_format);

-- Suspend previous task since following one is dependent
alter task epam_lab.raw.lineitem_data_from_stage suspend;
alter task epam_lab.raw.lineitem_raw_to_core suspend;

-- Create task for moving data from RAW to CORE_DWN area
create or replace task epam_lab.raw.lineitem_raw_to_core
	warehouse = 'COMPUTE_WH'
    after epam_lab.raw.lineitem_data_from_stage
    as
insert into epam_lab.core_dwh.lineitem(l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
select l_orderkey as l_orderkey
, l_partkey as l_partkey
, l_suppkey as l_suppkey
, l_linenumber as l_linenumber
, l_quantity as l_quantity
, REPLACE(l_extendedprice, ',', '.')::float as l_extendedprice
, REPLACE(l_discount, ',', '.')::float as l_discount
, REPLACE(l_tax, ',', '.')::float as l_tax
, TRIM(l_returnflag, '"') as l_returnflag
, TRIM(l_linestatus, '"') as l_linestatus
, TO_DATE(l_shipdate, 'DD.MM.YY') as l_shipdate
, TO_DATE(l_commitdate, 'DD.MM.YY') as l_commitdate
, TO_DATE(l_receiptdate, 'DD.MM.YY') as l_receiptdate
, TRIM(l_shipinstruct, '" ') as l_shipinstruct
, TRIM(l_shipmode, '" ') as l_shipmode
, TRIM(l_comment, '" .') as l_comment
from epam_lab.raw.stream_lineitem
where metadata$action = 'INSERT';

-- Resume previous task
alter task epam_lab.raw.lineitem_raw_to_core resume;
alter task epam_lab.raw.lineitem_data_from_stage resume;

-- Execute task which will trigger the following task after completion
execute task epam_lab.raw.lineitem_data_from_stage;

-- Check that we have data in RAW area
select * from epam_lab.raw.lineitem limit 100;

-- Check that we have data in CORE_DWH area
select * from epam_lab.core_dwh.lineitem limit 100;
