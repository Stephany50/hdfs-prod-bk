---etape 8 : MAJ des lignes au rccm absent avec les info des lignes au ns present.
insert into TMP.tt_flotte4_RCCM
select msisdn, NUMERO_REGISTRE_COMMERCE, compte_client_structure, DATE_SOUSCRIPTION
from (
select a.msisdn, b.NUMERO_REGISTRE_COMMERCE, a.compte_client_structure, a.DATE_SOUSCRIPTION,
row_number() over(partition by a.msisdn order by a.DATE_SOUSCRIPTION) as rang
from
(select x1.*
from (select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) not like '1.%'
and (trim(NUMERO_REGISTRE_COMMERCE) = '' or NUMERO_REGISTRE_COMMERCE is null or
(trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(NUMERO_REGISTRE_COMMERCE)) < 2)) x1) a
join (select x1.* from
(select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) not like '1.%'
and not(trim(NUMERO_REGISTRE_COMMERCE) = '' or NUMERO_REGISTRE_COMMERCE is null or
(trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(NUMERO_REGISTRE_COMMERCE)) < 2)) x1) b
on substr(trim(a.compte_client_structure),1,6) = substr(trim(b.compte_client_structure),1,6)
) c where rang = 1
union all
select msisdn, NUMERO_REGISTRE_COMMERCE, compte_client_structure, DATE_SOUSCRIPTION
from (
select a.msisdn, b.NUMERO_REGISTRE_COMMERCE, a.compte_client_structure, a.DATE_SOUSCRIPTION,
row_number() over(partition by a.msisdn order by a.DATE_SOUSCRIPTION) as rang
from
(select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) like '1.%'
and (trim(NUMERO_REGISTRE_COMMERCE) = '' or NUMERO_REGISTRE_COMMERCE is null or
(trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(NUMERO_REGISTRE_COMMERCE)) < 2)
) a
join (select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) like '1.%'
and not(trim(NUMERO_REGISTRE_COMMERCE) = '' or NUMERO_REGISTRE_COMMERCE is null or
(trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(trim(NUMERO_REGISTRE_COMMERCE),'0123456789\\!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = '') or
length(trim(NUMERO_REGISTRE_COMMERCE)) < 2)
) b
on trim(a.compte_client_structure) = trim(b.compte_client_structure)
) c where rang = 1
