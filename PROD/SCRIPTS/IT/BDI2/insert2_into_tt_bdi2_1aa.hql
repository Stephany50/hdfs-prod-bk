insert into TMP.tt_bdi2_1aa
SELECT distinct FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.MSISDN)) as msisdn
FROM (select distinct msisdn from TMP.TT_bdi1_1A where not(msisdn is null or trim(msisdn) = '')) A
join (select distinct msisdn from TMP.TT_BDI_FLOTTE_1A where not(msisdn is null or trim(msisdn) = '')) B
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.MSISDN)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.MSISDN))