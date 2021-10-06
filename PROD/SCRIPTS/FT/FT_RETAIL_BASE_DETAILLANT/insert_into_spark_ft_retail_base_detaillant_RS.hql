insert into MON.SPARK_FT_RETAIL_BASE_DETAILLANT
select Parent_msisdn MSISDN, site_name
, case when ( SENDER_CATEGORY IN ('TN','TNT') and refill_type = 'RC')
or ( SENDER_CATEGORY IN ('WHA')  and refill_type = 'RC') then 'NOT_USED_RECEIVER'
else SENDER_CATEGORY||'_'||refill_type
end CATEGORY
, sum(refill_amount) refill_amount
, count(distinct sender_msisdn) msisdn_count    --sum(msisdn_count) msisdn_count
, 'RS' Source_type
, CURRENT_TIMESTAMP insert_date
, refill_date
from
(
select refill_date, SENDER_MSISDN, Parent as Parent_msisdn, sender_category, refill_type, sum(refill_amount) refill_amount
from
(
select SENDER_MSISDN, refill_date, SENDER_CATEGORY, refill_type, sum(refill_amount) refill_amount
from MON.SPARK_FT_REFILL
where refill_date = '###SLICE_VALUE###'   --'01/11/2019'-- and refill_date <= '31/05/2018'
AND REFILL_MEAN ='C2S'
AND REFILL_TYPE  in ('RC', 'PVAS')
--AND SENDER_CATEGORY IN ('INHSM','INSM','NPOS','ORNGPTNR','PPOS')
and termination_ind = '200'
group by SENDER_MSISDN, refill_date, SENDER_CATEGORY, refill_type
)F
LEFT JOIN
( --Extraction des informations liées à la hierachie d'un point de vente.
select child.Primary_msisdn as msisdn, Child.GEOGRAPHICAL_DOMAIN_CODE, Child.Geographical_domain_name, child.Channel_user_name, Parent.Primary_msisdn as Parent
, grdparent.Primary_msisdn as grdparent
from
(
select primary_msisdn, max(Channel_user_id) Channel_user_id, max(Parent_user_id) Parent_user_id, max(Owner_user_id) Owner_user_id
, max(CHANNEL_USER_NAME) CHANNEL_USER_NAME,  max(GEOGRAPHICAL_DOMAIN_CODE) GEOGRAPHICAL_DOMAIN_CODE, max(geographical_domain_name) geographical_domain_name
--, max(Parent.Primary_msisdn) as Parent, max(grdparent.Primary_msisdn) as grdparent
from CDR.SPARK_IT_ZEBRA_MASTER
where TRANSACTION_DATE = '###SLICE_VALUE###'   --'11/12/2019'
and user_status = 'Active'
group by primary_msisdn
) Child
LEFT JOIN
(
select primary_msisdn, max(Channel_user_id) Channel_user_id, max(Parent_user_id) Parent_user_id, max(Owner_user_id) Owner_user_id
, max(CHANNEL_USER_NAME) CHANNEL_USER_NAME, max(GEOGRAPHICAL_DOMAIN_CODE) GEOGRAPHICAL_DOMAIN_CODE, max(geographical_domain_name) geographical_domain_name
--, max(Parent.Primary_msisdn) as Parent, max(grdparent.Primary_msisdn) as grdparent
from CDR.SPARK_IT_ZEBRA_MASTER
where TRANSACTION_DATE = '###SLICE_VALUE###'   --'11/12/2019'
and user_status = 'Active'
group by primary_msisdn
) Parent
ON  Child.Parent_user_id = Parent.Channel_user_id
LEFT JOIN
(
select primary_msisdn, max(Channel_user_id) Channel_user_id, max(Parent_user_id) Parent_user_id, max(Owner_user_id) Owner_user_id
, max(CHANNEL_USER_NAME) CHANNEL_USER_NAME, max(GEOGRAPHICAL_DOMAIN_CODE) GEOGRAPHICAL_DOMAIN_CODE, max(geographical_domain_name) geographical_domain_name
--, max(Parent.Primary_msisdn) as Parent, max(grdparent.Primary_msisdn) as grdparent
from CDR.SPARK_IT_ZEBRA_MASTER
where TRANSACTION_DATE = '###SLICE_VALUE###'   --'11/12/2019'
and user_status = 'Active'
group by primary_msisdn
) grdParent
ON  Child.Owner_user_id = grdparent.channel_user_id
)M
ON F.SENDER_MSISDN = M.msisdn
group by  refill_date, SENDER_MSISDN, Parent, grdparent, sender_category, refill_type
) a
LEFT JOIN
(
select msisdn, max(site_name) site_name from MON.SPARK_FT_CLIENT_LAST_SITE_DAY    --FT_CLIENT_SITE_TRAFFIC_DAY
where event_date = '###SLICE_VALUE###'--'01/11/2019'
group by msisdn
) b
ON a.SENDER_MSISDN = b.msisdn
group by refill_date, Parent_msisdn, site_name
, case when ( SENDER_CATEGORY IN ('TN','TNT') and refill_type = 'RC')
or ( SENDER_CATEGORY IN ('WHA')  and refill_type = 'RC') then 'NOT_USED_RECEIVER'
else SENDER_CATEGORY||'_'||refill_type
end