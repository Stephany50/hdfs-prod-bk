insert into TMP.TT_SCANS_FANTAISISTES_AMELIORE
select msisdn, type_personne
from MON.SPARK_FT_BDI_AMELIORE
where event_date = to_date('###SLICE_VALUE###')
and MOTIF_REJET_BO in ('scan pas une piece didentit¿',
'scans multiples',
'scan multiples',
'aucun scan associ¿',
'scan absent',
'scan indisponible et noms absents',
'scan pas une pi¿ce',
'scans multiples',
'scan incomplet',
'scan pas une piece d`identit¿',
'sans nom et sans scan',
'scan pas une piece didentit¿¿',
'scan incomplet ',
'plusieurs scans associes',
'scan flou/illisible',
'aucun scan associ¿¿',
'scan incomplet',
'aucun scan associ¿¿¿','scan absent',
'scans multiples',
'scan multiples',
'scan incomplet',
'scan absent',
'scan flou',
'scan incomplet',
'sans nom et sans scan',
'aucun scan associe',
'scan pas une piece didentit¿¿¿',
'scan fantaisiste'
)