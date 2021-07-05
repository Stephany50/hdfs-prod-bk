CREATE TABLE DIM.SPARK_DT_COEFF_VAS (
    month_period VARCHAR(10)
    , coefficient double
)

truncate table DIM.SPARK_DT_COEFF_VAS

insert into DIM.SPARK_DT_COEFF_VAS values 
('01', 0.430108),
('02', 0.443234),
('03', 0.503584),
('04', 0.581210),
('05', 0.525972),
('06', 0.554421),
('07', 0.532297),
('08', 0.574268),
('09', 0.582694),
('10', 0.596635),
('11', 0.620182),
('12', 0.637427);


CREATE TABLE DIM.SPARK_DT_COEFF_VAS_REALISE (
    month_period VARCHAR(10)
    , coefficient double
)

insert into DIM.SPARK_DT_COEFF_VAS_REALISE values 
('05', 0.598904733);