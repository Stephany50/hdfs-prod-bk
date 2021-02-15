insert into  TMP.tt_flotte4_PIECE_REP
select msisdn, num_piece_representant_legal, compte_client_structure, DATE_SOUSCRIPTION
from (
select a.msisdn, b.num_piece_representant_legal, a.compte_client_structure, a.DATE_SOUSCRIPTION,
row_number() over(partition by a.msisdn order by a.DATE_SOUSCRIPTION) as rang
from
(select x1.*
from (select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) not like '1.%'
and (trim(num_piece_representant_legal) = '' or num_piece_representant_legal is NULL or
trim(num_piece_representant_legal) rlike '^(\\d)\\1+$' or
length(trim(num_piece_representant_legal)) > 21 or
trim(num_piece_representant_legal) like '112233445%' or
(trim(translate(lower(trim(num_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null and
trim(translate(lower(trim(num_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) <> '') or
(trim(translate(lower(trim(num_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(lower(trim(num_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = ''))) x1
left join (select distinct msisdn as msisdn_s from TMP.tt_flotte02_generique1) x2
on trim(x1.msisdn) = trim(x2.msisdn_s)
where x2.msisdn_s is null) a
join (select x1.* from
(select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) not like '1.%'
and not(trim(num_piece_representant_legal) = '' or num_piece_representant_legal is NULL or
trim(num_piece_representant_legal) rlike '^(\\d)\\1+$' or
length(trim(num_piece_representant_legal)) > 21 or
trim(num_piece_representant_legal) like '112233445%' or
(trim(translate(lower(trim(num_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null and
trim(translate(lower(trim(num_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) <> '') or
(trim(translate(lower(trim(num_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(lower(trim(num_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = ''))) x1
left join (select distinct msisdn as msisdn_s from TMP.tt_flotte02_generique1) x2
on trim(x1.msisdn) = trim(x2.msisdn_s)
where x2.msisdn_s is null) b
on substr(trim(a.compte_client_structure),1,6) = substr(trim(b.compte_client_structure),1,6)
) c where rang = 1
union all
select msisdn, num_piece_representant_legal, compte_client_structure, DATE_SOUSCRIPTION
from (
select a.msisdn, b.num_piece_representant_legal, a.compte_client_structure, a.DATE_SOUSCRIPTION,
row_number() over(partition by a.msisdn order by a.DATE_SOUSCRIPTION) as rang
from
(select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) like '1.%'
and (trim(num_piece_representant_legal) = '' or num_piece_representant_legal is NULL or
trim(num_piece_representant_legal) rlike '^(\\d)\\1+$' or
length(trim(num_piece_representant_legal)) > 21 or
trim(num_piece_representant_legal) like '112233445%' or
(trim(translate(lower(trim(num_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null and
trim(translate(lower(trim(num_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) <> '') or
(trim(translate(lower(trim(num_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(lower(trim(num_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = ''))
) a
join (select *
from TMP.tt_flotte3
where upper(trim(compte_client_structure)) like '1.%'
and not(trim(num_piece_representant_legal) = '' or num_piece_representant_legal is NULL or
trim(num_piece_representant_legal) rlike '^(\\d)\\1+$' or
length(trim(num_piece_representant_legal)) > 21 or
trim(num_piece_representant_legal) like '112233445%' or
(trim(translate(lower(trim(num_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) is not null and
trim(translate(lower(trim(num_piece_representant_legal)),'0123456789abcdefghijklmnopqrstuvwxyz-/',' ')) <> '') or
(trim(translate(lower(trim(num_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) is null or
trim(translate(lower(trim(num_piece_representant_legal)),'abcdefghijklmnopqrstuv\\wxyz!"#$%&\'()*+,-./:;<=>?@[]^_`{|}~',' ')) = ''))
) b
on trim(a.compte_client_structure) = trim(b.compte_client_structure)
) c where rang = 1