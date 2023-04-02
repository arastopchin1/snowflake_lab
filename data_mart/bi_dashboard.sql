CREATE OR REPLACE VIEW epam_lab.data_mart.power_bi_dashboard AS (
	SELECT data.customer_number, dn.n_name FROM (	
    	SELECT COUNT(DISTINCT c_custkey) as customer_number, f.n_nationkey
    	FROM epam_lab.data_mart.fact f
    	GROUP BY f.n_nationkey
    ) data
    LEFT JOIN epam_lab.data_mart.dim_nation dn
    ON data.n_nationkey = dn.n_nationkey
    ORDER BY data.customer_number DESC
);

select * from epam_lab.data_mart.power_bi_dashboard;