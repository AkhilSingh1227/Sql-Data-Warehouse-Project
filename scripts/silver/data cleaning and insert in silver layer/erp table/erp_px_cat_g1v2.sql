
select * from silver.crm_prd_info
select id from bronze.erp_px_cat_g1v2
where (select id from bronze.erp_px_cat_g1v2) != (select prd_key from silver.crm_prd_info)



/* checking for unwanted spaces */

select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

/* data is not having any unwanted spaces */

----------- data standardization & consistency 

select distinct 
cat
from bronze.erp_px_cat_g1v2

/* we have no null or other value */
select distinct 
subcat
from bronze.erp_px_cat_g1v2
/* we have no null or other value */
select distinct 
maintenance
from bronze.erp_px_cat_g1v2
/* we have no null or other value */

/* no transformation needed in this table so directly inserting this table */

/*=============================================================================*/


/* ======================******** INSERT INTO SILVER LAYER *********=========================*/
/*===================================silver.erp_px_cat_g1v2=====================================*/

/*=============================================================================*/

insert into silver.erp_px_cat_g1v2
(
id,
cat,
subcat,
maintenance
)
select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2


select * from silver.erp_px_cat_g1v2
