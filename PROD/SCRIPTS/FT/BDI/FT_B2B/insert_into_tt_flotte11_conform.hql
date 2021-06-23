--etape 17 au moins une ligne des entreprises conformes
insert into TMP.tt_flotte11
select *
from (
select a.*,
row_number() over(partition by substr(upper(trim(compte_client_structure)),1,6) order by date_souscription desc nulls last) as rang3
from TMP.tt_flotte10 a where est_conforme = 'OUI' and compte_client_structure like '4.%') b where b.rang3 = 1
