create or replace table epam_lab.data_mart.fact as (
    select l_orderkey
    , l.l_partkey
    , l.l_suppkey
    , c.c_custkey
    , n.n_nationkey
    from epam_lab.core_dwh.lineitem l
    left join epam_lab.core_dwh.orders o
    on l.l_orderkey = o.o_orderkey
    left join epam_lab.core_dwh.customer c
    on o.o_custkey = c.c_custkey
    left join epam_lab.core_dwh.h_nation n
    on c.c_nationkey = n.n_nationkey
);

create table epam_lab.data_mart.dim_nation as (
	select n.n_nationkey
    , n.n_name
    , n.n_comment
    , n.n_regionkey
    , r.r_name
    , r.r_comment
    from epam_lab.core_dwh.h_nation n
    left join epam_lab.core_dwh.h_region r
    on n.n_regionkey = r.r_regionkey
);

create or replace table epam_lab.data_mart.dim_orders as (
	select o_orderkey
    , o_orderstatus
    , o_totalprice
    , o_orderdate
    , o_orderpriority
    , o_clerk
    , o_shippriority
    , o_comment
	from epam_lab.core_dwh.orders
);

create or replace table epam_lab.data_mart.dim_customer as (
	select c_custkey
    , c_name
    , c_address
    , c_phone
    , c_acctbal
    , c_mktsegment
    , c_comment
    from epam_lab.core_dwh.customer
);

create or replace table epam_lab.data_mart.dim_supplier as (
	select s_suppkey
    , s_name
    , s_address
    , s_phone
    , s_acctbal
    , s_comment
    from epam_lab.core_dwh.supplier
);

create or replace table epam_lab.data_mart.dim_part as (
	select p_partkey
    , p_name
    , p_mfgr
    , p_brand
    , p_type
    , p_size
    , p_container
    , p_retailprice
    , p_comment
    from epam_lab.core_dwh.part
);

create or replace table epam_lab.data_mart.dim_lineitem as (
	select l_orderkey
    , l_linenumber
    , ps_availqty
    , ps_supplycost
    , ps_comment
    , l_quantity
    , l_extendedprice
    , l_discount
    , l_tax
    , l_returnflag
    , l_linestatus
    , l_shipdate
    , l_commitdate
    , l_receiptdate
    , l_shipinstruct
    , l_shipmode
    , l_comment
    from epam_lab.core_dwh.lineitem l
    left join epam_lab.core_dwh.partsupp ps
    on ps.ps_partkey = l.l_partkey and ps.ps_suppkey = l.l_suppkey
);