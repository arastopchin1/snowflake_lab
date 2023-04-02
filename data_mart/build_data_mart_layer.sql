-- Create a stored procedure to fill data mart layer
CREATE OR REPLACE PROCEDURE epam_lab.raw.fill_data_mart_layer()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
BEGIN
	-- fill fact table
    merge into epam_lab.data_mart.fact f
    using (
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
    ) dwh
    on f.l_orderkey = dwh.l_orderkey
    and f.l_partkey = dwh.l_partkey
    and f.l_suppkey = dwh.l_suppkey
    and f.c_custkey = dwh.c_custkey
    and f.n_nationkey = dwh.n_nationkey
    when not matched then
    insert (l_orderkey, l_partkey, l_suppkey, c_custkey, n_nationkey)
    	values
        (l_orderkey, l_partkey, l_suppkey, c_custkey, n_nationkey);

    -- fill dim_nation table
    merge into epam_lab.data_mart.dim_nation dn
    using (
    	select n.n_nationkey
        , n.n_name
        , n.n_comment
        , n.n_regionkey
        , r.r_name
        , r.r_comment
        from epam_lab.core_dwh.h_nation n
        left join epam_lab.core_dwh.h_region r
        on n.n_regionkey = r.r_regionkey
    ) n
    on n.n_nationkey = dn.n_nationkey
    when not matched then
    insert (n_nationkey, n_name, n_comment, n_regionkey, r_name, r_comment)
    	values
        (n_nationkey, n_name, n_comment, n_regionkey, r_name, r_comment);

    -- fill dim_orders table
    merge into epam_lab.data_mart.dim_orders do
    using (
    	select o_orderkey
        , o_orderstatus
        , o_totalprice
        , o_orderdate
        , o_orderpriority
        , o_clerk
        , o_shippriority
        , o_comment
    	from epam_lab.core_dwh.orders
    ) o
    on o.o_orderkey = do.o_orderkey
    when not matched then
    insert (o_orderkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
    	values
        (o_orderkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment);

    -- fill dim_customer table
    merge into  epam_lab.data_mart.dim_customer dc
    using (
    	select c_custkey
        , c_name
        , c_address
        , c_phone
        , c_acctbal
        , c_mktsegment
        , c_comment
        from epam_lab.core_dwh.customer
    ) c
    on c.c_custkey = dc.c_custkey
    when not matched then
    insert (c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_comment)
    	values
        (c_custkey, c_name, c_address, c_phone, c_acctbal, c_mktsegment, c_comment);
    
    -- fill dim_supplier table
    merge into epam_lab.data_mart.dim_supplier ds
    using (
    	select s_suppkey
        , s_name
        , s_address
        , s_phone
        , s_acctbal
        , s_comment
        from epam_lab.core_dwh.supplier
    ) s
    on ds.s_suppkey = s.s_suppkey
    when not matched then
    insert (s_suppkey, s_name, s_address, s_phone, s_acctbal, s_comment)
    	values 
        (s_suppkey, s_name, s_address, s_phone, s_acctbal, s_comment);
    truncate epam_lab.data_mart.dim_part;
    
    -- fill dim_lineitem table
    merge into epam_lab.data_mart.dim_lineitem dl
    using (
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
    ) l
    on l.l_orderkey = dl.l_orderkey
    when not matched then
    	insert (l_orderkey, l_linenumber, ps_availqty, ps_supplycost, ps_comment, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
    	values
    (l_orderkey, l_linenumber, ps_availqty, ps_supplycost, ps_comment, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment);

    -- fill dim_part table
	merge into epam_lab.data_mart.dim_part dp
    using (
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
    ) p
    on dp.p_partkey = p.p_partkey
    when not matched then 
    insert (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
    	values
        (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment);
    RETURN 'Finished filling data mart layer.';
END;

create or replace task epam_lab.raw.initiate_pipeline as 
select 1;

create or replace task epam_lab.raw.fill_data_mart_layer_task
	warehouse = 'COMPUTE_WH'
    after epam_lab.raw.customer_raw_to_core
    , epam_lab.raw.lineitem_raw_to_core
    , epam_lab.raw.nation_raw_to_core
    , epam_lab.raw.orders_raw_to_core
    , epam_lab.raw.part_raw_to_core
    , epam_lab.raw.partsupp_raw_to_core
    , epam_lab.raw.region_raw_to_core
    , epam_lab.raw.supplier_raw_to_core
as
	call epam_lab.raw.fill_data_mart_layer();

execute task epam_lab.raw.initiate_pipeline;
