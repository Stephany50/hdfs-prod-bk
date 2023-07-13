INSERT INTO CDR.SPARK_IT_TERRAIN_ELIGIBLE_TDD 
SELECT
    Id ,
    Firstname ,
    Lastname ,
    PhoneNumber ,
    Address ,
    CreatedOn ,
    CreatedBy ,
    LastModifiedOn ,
    LastModifiedBy ,
    IsDeleted ,
    QuarterId ,
    CoverageType ,
    CustomerType ,
    PlatformName ,
    Profession ,
    ConfirmCoverageType ,
    IdSecond,
    Label ,
    CityId ,
    QuartersCreatedOn ,
    QuartersCreatedBy ,
    QuartersLastModifiedOn ,
    QuartersLastModifiedBy ,
    QuartersIsDeleted,
    original_file_name,
    To_date(From_unixtime(Unix_timestamp(Substring (original_file_name, -18,8),'yyyyMMdd'))) insert_date
FROM TT.SPARK_TT_TERRAIN_ELIGIBLE_TDD  C
LEFT JOIN (SELECT DISTINCT original_file_name FILE_NAME FROM CDR.SPARK_IT_TERRAIN_ELIGIBLE_TDD ) T 
ON T.file_name = C.original_file_name 
WHERE T.file_name IS NULL