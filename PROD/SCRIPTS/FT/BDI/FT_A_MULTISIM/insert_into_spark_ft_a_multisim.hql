INSERT INTO AGG.SPARK_FT_A_MULTISIM
(SELECT 
NBR_TOTAL_MULTISIM,NBR_TOTAL_NUM_PIECE,NBR_TOTAL_NUM_PIECE_VAL,
NBR_LIGNE_NUM_PIECE_VAL,NBR_TOTAL_NUM_PIECE_INVAL,NBR_LIGNE_NUM_PIECE_INVAL,
NBR_TOTAL_NOM_PRENOM,NBR_TOTAL_NOM_PRE_VAL,NBR_LIGNE_NOM_PRE_VAL,
NBR_TOTAL_NOM_PRE_INVAL,NBR_LIGNES_NOM_PRE_INVAL, 'TOTAL' TYPE_ROWS, 'NONE' ATTRIBUT,'NONE' VAL_ATTRIB,
'NONE' COUNT_ATTRIB,current_timestamp INSERT_DATE,'###SLICE_VALUE###' EVENT_DATE
FROM 
(SELECT 
    count(*) NBR_TOTAL_MULTISIM,
    count(distinct numero_piece) NBR_TOTAL_NUM_PIECE,
    count(distinct trim(upper(trim(nom)||' '||trim(prenom)))) NBR_TOTAL_NOM_PRENOM
FROM MON.SPARK_FT_BDI a where event_date = '###SLICE_VALUE###' and multi_sim='OUI' ) agg,
(select
count(distinct trim(upper(trim(nom)||' '||trim(prenom)))) NBR_TOTAL_NOM_PRE_INVAL,
count(*) NBR_LIGNES_NOM_PRE_INVAL
from MON.SPARK_FT_BDI where event_date = '###SLICE_VALUE###' and multi_sim='OUI' and 
(trim(upper(trim(nom)||' '||trim(prenom))) = "SANS PRENOM" or trim(upper(trim(nom)||' '||trim(prenom))) = "MA" or 
trim(upper(trim(nom)||' '||trim(prenom))) = "N/A" or trim(upper(trim(nom)||' '||trim(prenom))) = "PRENOM" or
trim(upper(trim(nom)||' '||trim(prenom))) = "-" or length(trim(upper(trim(nom)||' '||trim(prenom)))) < 3 or 
not(trim(upper(trim(nom)||' '||trim(prenom))) rlike '[A-Za-z]'))
)nomp_inv,
(select
count(distinct trim(upper(trim(nom)||' '||trim(prenom)))) NBR_TOTAL_NOM_PRE_VAL,
count(*) NBR_LIGNE_NOM_PRE_VAL
from MON.SPARK_FT_BDI where event_date = '###SLICE_VALUE###' and multi_sim='OUI' and 
not(trim(upper(trim(nom)||' '||trim(prenom))) = "SANS PRENOM" or trim(upper(trim(nom)||' '||trim(prenom))) = "MA" or 
trim(upper(trim(nom)||' '||trim(prenom))) = "N/A" or trim(upper(trim(nom)||' '||trim(prenom))) = "PRENOM" or
trim(upper(trim(nom)||' '||trim(prenom))) = "-" or length(trim(upper(trim(nom)||' '||trim(prenom)))) < 3 or 
not(trim(upper(trim(nom)||' '||trim(prenom))) rlike '[A-Za-z]'))
)nomp_val,
(select
count(distinct numero_piece) NBR_TOTAL_NUM_PIECE_INVAL,
count(*) NBR_LIGNE_NUM_PIECE_INVAL
from MON.SPARK_FT_BDI where event_date = '###SLICE_VALUE###' and multi_sim='OUI' and 
(length(nvl(numero_piece,'')) < 4 or not(trim(numero_piece) rlike '^[0-9A-Za-z]') or not(trim(numero_piece) rlike '[2-9A-Za-z]+') 
or trim(numero_piece) like '112233445%' or trim(numero_piece) like '0123456789%' or length(nvl(numero_piece,'')) > 21 or trim(numero_piece)='NULL')
)nump_inv,
(select
count(distinct numero_piece) NBR_TOTAL_NUM_PIECE_VAL,
count(*) NBR_LIGNE_NUM_PIECE_VAL
from MON.SPARK_FT_BDI where event_date = '###SLICE_VALUE###' and multi_sim='OUI'  and 
not(length(nvl(numero_piece,'')) < 4 or not(trim(numero_piece) rlike '^[0-9A-Za-z]') or not(trim(numero_piece) rlike '[2-9A-Za-z]+') 
or trim(numero_piece) like '112233445%' or trim(numero_piece) like '0123456789%' or length(nvl(numero_piece,'')) > 21 or trim(numero_piece)='NULL')
)nump_val)
UNION
(-----Top des nom prenom
SELECT   
0 NBR_TOTAL_MULTISIM,0 NBR_TOTAL_NUM_PIECE,0 NBR_TOTAL_NUM_PIECE_VAL,
0 NBR_LIGNE_NUM_PIECE_VAL,0 NBR_TOTAL_NUM_PIECE_INVAL,0 NBR_LIGNE_NUM_PIECE_INVAL,
0 NBR_TOTAL_NOM_PRENOM,0 NBR_TOTAL_NOM_PRE_VAL,0 NBR_LIGNE_NOM_PRE_VAL,
0 NBR_TOTAL_NOM_PRE_INVAL,0 NBR_LIGNES_NOM_PRE_INVAL, 'NONE' TYPE_ROWS, 'NOM' ATTRIBUT,a.VAL_ATTRIB,
a.COUNT_ATTRIB,current_timestamp INSERT_DATE,'###SLICE_VALUE###' EVENT_DATE
FROM (select trim(upper(trim(nom)||' '||trim(prenom))) VAL_ATTRIB,count(*) COUNT_ATTRIB from MON.SPARK_FT_BDI where event_date = '###SLICE_VALUE###' and multi_sim='OUI'
group by trim(upper(trim(nom)||' '||trim(prenom))) order by count(*) desc limit 20) a
)
UNION
(-----Top des numéro pieces
SELECT   
0 NBR_TOTAL_MULTISIM,0 NBR_TOTAL_NUM_PIECE,0 NBR_TOTAL_NUM_PIECE_VAL,
0 NBR_LIGNE_NUM_PIECE_VAL,0 NBR_TOTAL_NUM_PIECE_INVAL,0 NBR_LIGNE_NUM_PIECE_INVAL,
0 NBR_TOTAL_NOM_PRENOM,0 NBR_TOTAL_NOM_PRE_VAL,0 NBR_LIGNE_NOM_PRE_VAL,
0 NBR_TOTAL_NOM_PRE_INVAL,0 NBR_LIGNES_NOM_PRE_INVAL, 'NONE' TYPE_ROWS, 'PIECE' ATTRIBUT,a.VAL_ATTRIB,
a.COUNT_ATTRIB,current_timestamp INSERT_DATE,'###SLICE_VALUE###' EVENT_DATE
FROM (select numero_piece VAL_ATTRIB,count(*) COUNT_ATTRIB from MON.SPARK_FT_BDI where event_date = '###SLICE_VALUE###' and multi_sim='OUI'
group by numero_piece order by count(*) desc limit 20) a
)
