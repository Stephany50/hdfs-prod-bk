insert into mon.omny_new_account
 select a.NamePrefix  ,a.FirstName  ,a.LastName  ,a.Msisdn  ,a.IdentificationNumber  ,a.FormNumber  ,a.DateofBirth  ,a.Gender  ,a.Adress
,a.District  ,a.City  ,a.State  ,a.Country  ,a.Description  ,a.PreferredLanguage  ,a.TypeofIdentityProof  ,a.MobileGroupRoleId
,a.GradeCode  ,a.TCPProfileId  ,a.PrimaryAccount        ,a.CustomerId  ,a.AccountNumber  ,a.AccountType  ,a.WalletType
,a.UserStatus  ,a.MiddleName  ,a.Nationality  ,a.IDType  ,a.IDNumber  ,a.PlaceOfIDIssued  ,a.IssuedCountryCode
,a.ResidencyCountryCode  ,a.IssuedDate  ,a.IsIDExpires  ,a.ExpireDate   ,a.PostalCode  ,a.EmployerName , current_timestamp , "###SLICE_VALUE###"
from
(select a.NamePrefix  ,a.FirstName  ,a.LastName  ,a.Msisdn  ,a.IdentificationNumber  ,a.FormNumber  ,a.DateofBirth  ,a.Gender  ,a.Adress
,a.District  ,a.City  ,a.State  ,a.Country  ,a.Description  ,a.PreferredLanguage  ,a.TypeofIdentityProof  ,a.MobileGroupRoleId
,a.GradeCode  ,a.TCPProfileId  ,a.PrimaryAccount        ,a.CustomerId  ,a.AccountNumber  ,a.AccountType  ,a.WalletType
,a.UserStatus  ,a.MiddleName  ,a.Nationality  ,a.IDType  ,a.IDNumber  ,a.PlaceOfIDIssued  ,a.IssuedCountryCode
,a.ResidencyCountryCode  ,a.IssuedDate  ,a.IsIDExpires  ,a.ExpireDate   ,a.PostalCode  ,a.EmployerName
from
(select (Case when Titre like 'Made%' then 'Mlle'
            when Titre  like 'Mada%' then 'Mme'
            else 'Mr' end) NamePrefix
          ,Prenomduclient FirstName
          ,Nomduclient LastName
          ,Telephone Msisdn
          ,Numeropiece IdentificationNumber
          ,Numero FormNumber
          ,DatedeNaissance DateofBirth
          ,(Case when Titre like 'Made%' then 'Female'
            when Titre  like 'Mada%' then 'Female'
            else 'Male' end) Gender
           ,Quartier Adress
           ,Null District
           ,Null City
           ,Ville State
           ,'Cameroun' Country
           ,Null Description
           ,'Français' PreferredLanguage
           ,Piece  TypeofIdentityProof
           ,'SUBS_ROLE'  MobileGroupRoleId
           ,'SUBS'  GradeCode
           ,'TCP1012220307.000001' as TCPProfileId
           ,'Y' PrimaryAccount
          ,NULL CustomerId
          ,NULL AccountNumber
          ,NULL AccountType
          ,'A' WalletType
           ,'A'  UserStatus
          ,NULL  MiddleName
           ,NULL Nationality
           ,Piece IDType
           ,Numeropiece IDNumber
           ,Null PlaceOfIDIssued
           ,NULL IssuedCountryCode
           ,NULL ResidencyCountryCode
           ,to_date(Delivrance)  IssuedDate
          ,(Case when to_date(Expiration) > current_timestamp then 'NON' else 'OUI' end)  IsIDExpires
           ,to_date(Expiration) ExpireDate
          ,NULL  PostalCode
          ,NULL EmployerName
from CDR.SPARK_IT_NOMAD_CLIENT_DIRECTORY
 where original_file_date = date_add("###SLICE_VALUE###", 1) and TYPEDECONTRAT='Nouvel Abonnement'
 and  ETATDEXPORTGLOBAL ='SUCCESS' and LOGINVENDEUR not in ('testfo','NKOLBONG','testve'))  a left join
 (select Account_id msisdn
from cdr.spark_it_om_all_balance
where original_file_date =(select max(original_file_date) original_file_date
                             from cdr.spark_it_om_all_balance where original_file_date <= "###SLICE_VALUE###" ) and
Account_type ='Subscriber') b
on a.msisdn = b.msisdn
where b.msisdn is null) a left join
(select cni
from
(select distinct id_number cni
from cdr.spark_it_omny_user_data
union
select distinct id_number cni
from cdr.spark_it_omny_user_registration
where ACCOUNT_STATUS ='Y')) b
on a.IDENTIFICATIONNUMBER = b.cni
where b.cni is null