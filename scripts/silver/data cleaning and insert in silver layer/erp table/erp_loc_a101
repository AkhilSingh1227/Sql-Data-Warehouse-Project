select * from bronze.erp_loc_a101
select * from silver.erp_cust_az12

/* as checked the cid in <erp_cust_az12> is different from <erp_loc_a101> 
as the it contains a '-' sign in between */

select 
replace(cid,'-','') as cid,
cntry
from bronze.erp_loc_a101
--- data standardization & consistency
select distinct 
cntry
from bronze.erp_loc_a101
order by cntry


select distinct 
cntry,
case 
    when upper(trim(cntry))in ('DE','GERMANY') then 'Germany'
    when upper(trim(cntry))in ('US','USA','UNITED STATES') THEN 'United States'
    when upper(trim(cntry))in ('AUSTRALIA') then 'Australia'
    when upper(trim(cntry))in ('CANADA') then 'Canada'
    when upper(trim(cntry))in ('FRANCE') then 'France'
    when upper(trim(cntry))in ('UNITED KINGDOM') then 'United Kingdom'
    else 'n/a'
end as cntry_new
from bronze.erp_loc_a101
order by cntry

/* above we have cleaned both the tables */

/*=============================================================================*/


/* ======================******** INSERT INTO SILVER LAYER *********=========================*/
/*===================================silver.erp_loc_a101=====================================*/

/*=============================================================================*/
insert into silver.erp_loc_a101
(
cid,
cntry
)
select
replace(cid,'-','') as cid,--- handled invalid values
case 
    when upper(trim(cntry))in ('DE','GERMANY') then 'Germany'
    when upper(trim(cntry))in ('US','USA','UNITED STATES') THEN 'United States'
    when upper(trim(cntry))in ('AUSTRALIA') then 'Australia'
    when upper(trim(cntry))in ('CANADA') then 'Canada'
    when upper(trim(cntry))in ('FRANCE') then 'France'
    when upper(trim(cntry))in ('UNITED KINGDOM') then 'United Kingdom' --- data normalization and handle missing data 
    else 'n/a'
end as cntry
from bronze.erp_loc_a101


/*=============================================================================*/


/* ======================******** CHECKING SILVER LAYER *********=========================*/
/*===================================silver._loc_a101=====================================*/

/*=============================================================================*/

select distinct 
cntry
from silver.erp_loc_a101
order by cntry
select* from silver.erp_loc_a101
