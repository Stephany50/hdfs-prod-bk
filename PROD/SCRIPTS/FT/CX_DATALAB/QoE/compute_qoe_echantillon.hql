--#1- Extraction aletoire de 5K lignes par regions
--#2-Ranking des lignes
--#3- Jointure avec la table
--#cette table contient les regions et le nombre de lignes par region
create table tmp.cx_region_bus(
   region varchar(255),
   nbr_ligne bigint
);

---#Retrouver les libélés des regions
select distinct region_bus from DIM.SPARK_DT_GSM_CELL_CODE;

---#Insertion des regions avec la taille de la population par region
insert into tmp.cx_region_bus values('ADAMAOUA',1085);
insert into tmp.cx_region_bus values('LITTORAL',2783);
insert into tmp.cx_region_bus values('EST',969);
insert into tmp.cx_region_bus values('EXTREME-NORD',1954);
insert into tmp.cx_region_bus values('NORD',1515);
insert into tmp.cx_region_bus values('NORD-OUEST',590);
insert into tmp.cx_region_bus values('OUEST',1432);
insert into tmp.cx_region_bus values('SUD',876);
insert into tmp.cx_region_bus values('SUD-OUEST',660);
insert into tmp.cx_region_bus values('CENTRE',3136);

---#Genération des lignes à analyser sur cem
drop table tmp.cx_sample_data_cem;
create table tmp.cx_sample_data_cem
select a.served_msisdn msisdn
from (select served_msisdn,region,
ROW_NUMBER() OVER (PARTITION BY region ORDER BY rand()) AS rang 
FROM (select a.served_msisdn,b.region, count(*) nbr
from (select * from mon.spark_ft_msc_transaction 
where transaction_date between '2022-10-31' and '2022-11-06' and transaction_direction = 'Sortant'
and length(served_msisdn)=9 and served_msisdn like "6%"
) a left join (select ci, lac,max(region_bus) region 
from DIM.SPARK_DT_GSM_CELL_CODE group by ci, lac) b 
on lpad(trim(Substr(a.served_party_location, 14, 5)), 5, 0)=lpad(trim(b.ci), 5, 0) and 
lpad(trim(Substr(a.served_party_location, -11, 5)), 5, 0)=lpad(trim(b.lac), 5, 0)
group by served_msisdn, region) T)a
inner join tmp.cx_region_bus b on upper(a.region) = upper(b.region)
where a.rang <=b.nbr_ligne
group by msisdn; 



hive --hiveconf tez.queue.name=compute --showHeader=false --outputFormat=csv2 -e "select msisdn from tmp.cx_sample_data_cem" > sample_qoe.csv