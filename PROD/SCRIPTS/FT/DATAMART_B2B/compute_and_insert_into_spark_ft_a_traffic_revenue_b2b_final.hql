INSERT INTO AGG.SPARK_TRAFFIC_REVENUE_B2B_FINAL

SELECT 
PRODUCT,
PRODUCT_DESCRIPTION,
PROFIL,
MAIN_PRODUCT,
OFFER,
REVENUE,
EVENT_DATE
FROM AGG.SPARK_TRAFFIC_REVENUE_B2B
WHERE  EVENT_DATE = '2023-01-01'
AND upper(trim(OFFER)) IN
(
upper(trim("Pro Flex plus")),
upper(trim("FLEX PLUS ")),
upper(trim("Forfait Mix")),
upper(trim("Pro Flex")),
upper(trim("Pro/Access")),
upper(trim("PLUS PLUS")),
upper(trim("FLEX")),
upper(trim("Pro/Access plus")),
upper(trim("SCHLUMBERGER")),
upper(trim("FLEX PLUS")),
upper(trim("Forfait data")),
upper(trim("FLEX  PLUS")),
upper(trim("FLEX PLUS 5K UN")),
upper(trim("ORANGE COMMUNAUTE SOHO")),
upper(trim("BUNDLE PLUS")),
upper(trim("MIX")),
upper(trim("PLATINUM ")),
upper(trim("ONE PLUS")),
upper(trim("SOHO PLUS")),
upper(trim("COMMUNITY PLUS")),
upper(trim("PRO PLUS")),
upper(trim("Flex Plus 20K UN")),
upper(trim("Flex Plus 10K UN")),
upper(trim("CLOUD")),
upper(trim("MOBILITY")),
upper(trim("Flex Plus 30K UN")),
upper(trim("MAGIC PLUS")),
upper(trim("Offre data")),
upper(trim("CLOUD INTENSE")),
upper(trim("Orange Pro L")),
upper(trim("ATHOS")),
upper(trim("HONORABLE PLUS")),
upper(trim("Orange Pro M")),
upper(trim("DATA LIVE MIX BUNDLE L1")),
upper(trim("DATA LIVE 5MO")),
upper(trim("DATA LIVE MIX SMARTRACK")),
upper(trim("CLOUD MOIS ++"))
)