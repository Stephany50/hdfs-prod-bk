INSERT INTO TMP.TT_bdi2_1A
select a.*
from TMP.TT_bdi1_1A a
left join TMP.TT_BDI2_1AA  b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.MSISDN)) = trim(b.msisdn)
where b.msisdn is null