insert into AGG.SPARK_FT_A_KPI_TANGO_TELCO
select *
from 
(select count(distinct msisdn) as kpi1
from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO
where event_date='###SLICE_VALUE###') A,
(select count(distinct msisdn) as kpi2
from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO
where event_date='###SLICE_VALUE###'
and upper(trim(nom_prenom_om)) = upper(trim(nom_prenom_telco))) B,
(select count(distinct msisdn) as kpi3
from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO
where event_date='###SLICE_VALUE###'
and upper(trim(numero_piece_om)) = upper(trim(numero_piece_telco))) C,
(select count(distinct msisdn) as kpi4
from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO
where event_date='###SLICE_VALUE###'
and date_naissance_om = date_naissance_telco) D,
(select count(distinct msisdn) as kpi5
from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO
where event_date='###SLICE_VALUE###'
and upper(trim(nom_prenom_om)) = upper(trim(nom_prenom_telco))
and upper(trim(numero_piece_om)) = upper(trim(numero_piece_telco))) E,
(select count(distinct msisdn) as kpi6
from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO
where event_date='###SLICE_VALUE###'
and upper(trim(nom_prenom_om)) = upper(trim(nom_prenom_telco))
and upper(trim(numero_piece_om)) = upper(trim(numero_piece_telco))
and date_naissance_om = date_naissance_telco) F,
(select count(distinct msisdn) as kpi7
from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO
where event_date='###SLICE_VALUE###'
and upper(trim(nom_prenom_om)) = upper(trim(nom_prenom_telco))
and substr(upper(trim(numero_piece_om)),1,9) = substr(upper(trim(numero_piece_telco)),1,9)) G,
(select count(distinct msisdn) as kpi8
from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO
where event_date='###SLICE_VALUE###'
and upper(trim(nom_prenom_om)) = upper(trim(nom_prenom_telco))
and substr(upper(trim(numero_piece_om)),1,9) = substr(upper(trim(numero_piece_telco)),1,9)
and date_naissance_om = date_naissance_telco) H,
(select 
sum(case when trim(SIMILARITY_STATUS) = 'OUI' and  upper(trim(numero_piece_om)) = upper(trim(numero_piece_telco)) and to_date(date_naissance_om) = to_date(date_naissance_telco) then 1 else 0 end) kpi9,
sum(case when trim(SIMILARITY_STATUS) = 'OUI' and substr(upper(trim(numero_piece_om)),1,9) = substr(upper(trim(numero_piece_telco)),1,9) and to_date(date_naissance_om) = to_date(date_naissance_telco) then 1 else 0 end) kpi10,
current_timestamp INSERT_DATE,'###SLICE_VALUE###' EVENT_DATE
from (select
a.*,
(case when a.SIMILARITY_VALUE < 95.00 then 'NON' else 'OUI' end) SIMILARITY_STATUS
from (select MSISDN,NOM_PRENOM_OM,NOM_PRENOM_TELCO,NUMERO_PIECE_OM,NUMERO_PIECE_TELCO,date_naissance_telco,
date_naissance_om,compute_similarity(NOM_PRENOM_OM||"¤£"||NOM_PRENOM_TELCO) SIMILARITY_VALUE
from MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO where event_date='###SLICE_VALUE###') A)) J


