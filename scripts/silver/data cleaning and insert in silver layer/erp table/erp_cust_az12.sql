select * from bronze.erp_cust_az12
/* ---------------------------------------------------------------------
checking data for cid
it is observed that cis can be linked with cst_key from crm_cust_info table
but it contains NAS a the begining

we need to clean the data by removing the NAS so we can use the data for connection to other table
------------------------------------------------------------------------------------------*/
select 
cid,
case when cid like 'NAS%' then
substring(cid,4,len(cid))
else cid
end cid

from bronze.erp_cust_az12

/* we have successfully removed the NAS from the cid */

/* we need to check for date is it is a valid date */

select bdate from bronze.erp_cust_az12
where bdate > getdate()

/* we can find 16 records with the date greter than the curret date */

/* lets check for the last column gen */

select distinct 
gen from
bronze.erp_cust_az12

/* as checked we have 6 value i.e. null, blank, f, m, male, female */
select DISTINCT
gen,
case when upper(trim(gen)) in ('M','MALE') then 'Male' ------ DATA STANDARIZATION
 when upper(trim(gen)) IN ('F', 'FEMALE') then 'Female'
else 'n/a'
end as gen
from
bronze.erp_cust_az12;

/*=============================================================================*/


/* ======================******** INSERT INTO SILVER LAYER *********=========================*/
/*===================================silver.ERP_CUST_AZ12=====================================*/

/*=============================================================================*/
insert into silver.erp_cust_az12
(
cid,
bdate,
gen
)
select
case 
     when cid like 'NAS%' then substring(cid,4,len(cid)) ------ removed nas prefix if present
     else cid
end as cid,
case 
     when bdate > getdate() then null ------ set future date to null
     else bdate
end as bdate,
case 
     when upper(trim(gen)) in ('M','MALE') then 'Male' ------ DATA normalization
     when upper(trim(gen)) IN ('F', 'FEMALE') then 'Female'
     else 'n/a'
end as gen ------- normalize gender values and handle missing data 
from bronze.erp_cust_az12


/*=============================================================================*/


/* ======================******** CHECKING SILVER LAYER *********=========================*/
/*===================================silver.ERP_CUST_AZ12=====================================*/

/*=============================================================================*/


/* checking CID */
select 
cid
from silver.erp_cust_az12
where cid like 'NAS%'
/* no cid found with NAS */

/* checking bdate */
select bdate from silver.erp_cust_az12
where bdate > getdate()
/* no invalid date found */

/*checking gen */

select distinct
gen
from silver.erp_cust_az12

/* as checked we have 3 value i.e. n/a, male, female */

select * from silver.erp_cust_az12
