INSERT INTO AGG.SPARK_FT_A_NVL_ACQUISITIONS_TELCO  
SELECT R.key,R.value,current_timestamp() AS insert_date,'###SLICE_VALUE###' AS EVENT_DATE
FROM (SELECT A.*, B.*
FROM 
( ----- les nvl acquisitions telco
    SELECT 
    count(DISTINCT A.msisdn) acq_total,
    SUM(case when trim(b.est_snappe)='OUI' then 1 else 0 end) acq_valide_bot,
    SUM(case when trim(b.est_snappe)<>'OUI' then 1 else 0 end) acq_non_valide_bot
    FROM (SELECT * FROM MON.SPARK_FT_KYC_BDI_PP WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') 
    AND TO_DATE(date_activation) = TO_DATE('###SLICE_VALUE###')) A
    LEFT JOIN (SELECT DISTINCT msisdn,est_snappe FROM DIM.SPARK_DT_BASE_IDENTIFICATION) B on A.msisdn = B.msisdn) A,
    ( ---- acquisitions avec compte OM
    SELECT COUNT(DISTINCT A.msisdn) inter_total,
    SUM(case when A.statut_bo_om='OUI' then 1 else 0 end) inter_valide_bom,
    SUM(case when A.statut_bo_om<>'OUI' then 1 else 0 end) inter_non_valide_bom,
    SUM(case when to_date(A.date_creation_om) >= to_date(A.date_activation) then 1 else 0 end) inter_recycle,
    SUM(case when to_date(A.date_creation_om) < to_date(A.date_activation) then 1 else 0 end) inter_non_recycle, ---avec un vieux compte om
    SUM(case when upper(trim(nom_prenom_om)) = upper(trim(nom_prenom_telco)) then 1 else 0 end) inter_nom,
    SUM(case when upper(trim(numero_piece_om)) = upper(trim(numero_piece_telco)) then 1 else 0 end) inter_naiss,
    SUM(case when to_date(date_naissance_om) = to_date(date_naissance_telco) then 1 else 0 end) inter_piece,
    SUM(case when A.similarity_value >= 95.00 and to_date(A.date_naissance_om) = to_date(A.date_naissance_telco) and 
    substr(upper(trim(A.numero_piece_om)),1,9) = substr(upper(trim(A.numero_piece_telco)),1,9)  then 1 else 0 end) inter_all,

    ----- nouvelles acquisitions OM  
    SUM(case when to_date(A.date_creation_om) = TO_DATE('###SLICE_VALUE###') then 1 else 0 end) om_acq_total,
    SUM(case when A.statut_bo_om='OUI' and to_date(A.date_creation_om) = TO_DATE('###SLICE_VALUE###') then 1 else 0 end) om_acq_valide_bom,
    SUM(case when A.statut_bo_om<>'OUI' and to_date(A.date_creation_om) = TO_DATE('###SLICE_VALUE###') then 1 else 0 end) om_acq_non_valide_bom,
    SUM(case when upper(trim(A.nom_prenom_om)) = upper(trim(A.nom_prenom_telco)) and to_date(A.date_creation_om) = TO_DATE('###SLICE_VALUE###') then 1 else 0 end) om_inter_nom,
    SUM(case when upper(trim(numero_piece_om)) = upper(trim(numero_piece_telco)) and to_date(A.date_creation_om) = TO_DATE('###SLICE_VALUE###') then 1 else 0 end) om_inter_naiss,
    SUM(case when to_date(date_naissance_om) = to_date(date_naissance_telco) and to_date(A.date_creation_om) = TO_DATE('###SLICE_VALUE###') then 1 else 0 end) om_inter_piece,
    SUM(case when A.similarity_value >= 95.00 and to_date(A.date_naissance_om) = to_date(A.date_naissance_telco) and 
    substr(upper(trim(A.numero_piece_om)),1,9) = substr(upper(trim(A.numero_piece_telco)),1,9) and to_date(A.date_creation_om) = TO_DATE('###SLICE_VALUE###') then 1 else 0 end) om_inter_all

    FROM (SELECT *,compute_similarity(nom_prenom_om||"¤£"||nom_prenom_telco) similarity_value
    FROM MON.SPARK_FT_ALIGNEMENT_TANGO_TELCO  WHERE event_date=TO_DATE('###SLICE_VALUE###')) A
    INNER JOIN (SELECT msisdn,date_acquisition,date_activation FROM MON.SPARK_FT_KYC_BDI_PP WHERE TO_DATE(event_date)=TO_DATE('###SLICE_VALUE###') 
    AND TO_DATE(date_activation) = TO_DATE('###SLICE_VALUE###')) B ON A.msisdn = B.msisdn) B
) LATERAL VIEW EXPLODE(MAP(
    'telco_acq_total',acq_total,
    'telco_acq_valide_bot',acq_valide_bot,
    'telco_acq_non_valide_bot',acq_non_valide_bot,
    'telco_inter_total',inter_total,
    'telco_inter_valide_bom',inter_valide_bom,
    'telco_inter_non_valide_bom',inter_non_valide_bom,
    'telco_inter_recycle',inter_recycle,
    'telco_inter_non_recycle',inter_non_recycle,
    'telco_inter_nom',inter_nom,
    'telco_inter_naiss',inter_naiss,
    'telco_inter_piece',inter_piece,
    'telco_inter_all',inter_all,
    'om_acq_total',om_acq_total,
    'om_acq_valide_bom',om_acq_valide_bom,
    'om_acq_non_valide_bom',om_acq_non_valide_bom,
    'om_inter_nom',om_inter_nom,
    'om_inter_naiss',om_inter_naiss,
    'om_inter_piece',om_inter_piece,
    'om_inter_all',om_inter_all
)) R as key, value

