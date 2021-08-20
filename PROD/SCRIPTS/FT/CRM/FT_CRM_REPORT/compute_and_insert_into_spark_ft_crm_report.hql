insert into mon.spark_ft_crm_reporting
SELECT
	msisdn,
	segment,
	sous_segment,
	date_interaction,
	categorie,
	typarticle,
	article,
	motif,
	count(*) total,
	ville,
	region,
	CURRENT_TIMESTAMP() INSERT_DATE,
    '###SLICE_VALUE###' EVENT_DATE
from
(
	select
		A.msisdn msisdn,
		segment,
		sous_segment,
		date_interaction,
		categorie,
		typarticle,
		article,
		motif,
		nvl(townname_B, townname_A) ville,
		nvl(ADMINISTRATIVE_REGION_B, ADMINISTRATIVE_REGION_A) region
	FROM 
	(
		SELECT 
			trim(onc_numeroappelant) msisdn,
			categorie,
			date_interaction,
			typarticle,
			article,
			motif
		from cdr.spark_it_crm_reporting 
		where original_file_date='###SLICE_VALUE###' 
			and onc_numeroappelant is not null and 
			length(GET_NNP_MSISDN_9DIGITS(trim(onc_numeroappelant)))=9
	) A
	left join
	(
		select
			T.msisdn msisdn,
			segment,
			case
				when segment = 'B2B' then sous_segment_b2b
				when segment = 'B2C' then sous_segment_b2c
				else null
			end sous_segment
		from
		(
			select
				MSISDN,
				upper(C.SEGMENTATION) segment
			from
			(
				SELECT
					ACCESS_KEY MSISDN,
					COMMERCIAL_OFFER
				FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
				WHERE EVENT_DATE = DATE_ADD('###SLICE_VALUE###',1) AND OSP_STATUS IN ('ACTIVE', 'INACTIVE')
			) B1
			LEFT JOIN DIM.DT_OFFER_PROFILES C ON UPPER(B1.COMMERCIAL_OFFER) = UPPER(C.PROFILE_CODE)
			WHERE UPPER(C.SEGMENTATION) IN ('B2B', 'B2C') 
		) T
		left join
		(
			select
				msisdn,
				case 
					when arpu <= 0 then 'zero'
					when arpu > 0 and arpu <= 1000 then 'low'
					when arpu >= 1000 and arpu < 5000 then 'medium'
					else 'high'
				end sous_segment_b2c,
				case 
					when arpu < 100000/30 then 'tpe'
					when arpu >= 100000/30 and arpu < 1000000/30 then 'pme'
					else 'gc'
				end sous_segment_b2b
			from MON.SPARK_FT_CBM_ARPU_MOU
			where EVENT_DATE = '###SLICE_VALUE###' 
		) U 
		on T.msisdn = U.msisdn
	) B
	on A.msisdn=B.MSISDN
	LEFT JOIN 
	(
		SELECT
			A.MSISDN,
			MAX(A.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_A,
			MAX(B.ADMINISTRATIVE_REGION) ADMINISTRATIVE_REGION_B,
			MAX(A.townname) townname_A,
			MAX(B.townname) townname_B
		FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY A
		LEFT JOIN (
			SELECT * FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY WHERE EVENT_DATE='###SLICE_VALUE###'
		) B 
		ON A.MSISDN = B.MSISDN
		WHERE A.EVENT_DATE='###SLICE_VALUE###'
		GROUP BY A.MSISDN
	) SITE 
	ON SITE.MSISDN = A.msisdn
) T
group by
	msisdn,
	segment,
	sous_segment,
	date_interaction,
	categorie,
	typarticle,
	article,
	motif,
	ville,
	region