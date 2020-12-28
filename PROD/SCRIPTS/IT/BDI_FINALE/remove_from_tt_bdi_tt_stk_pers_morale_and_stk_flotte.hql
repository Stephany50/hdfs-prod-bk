INSERT INTO TMP.TT_bdi2
select a.*
from TMP.TT_bdi1 a
left join TMP.TT_BDI2_1AA0  b
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.MSISDN)) = trim(b.msisdn)
where b.msisdn is null