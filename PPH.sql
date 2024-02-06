/***************************************************************************************************
Author: Allison McCoy
Created: 2022-04
Modified:	2024-02
Purpose: The purpose of the project is to define and validate risk factors of both all-cause 
			PPH and atonic PPH within the EHR
Keywords: Postpartum hemorrhage, PPH, obstetrics, anesthesia, pregnancy, delivery
****************************************************************************************************/

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED -- try not to use up all the Clarity resources

/***************************************************************************************************
	Study period
****************************************************************************************************/

DECLARE
 @StartDt	date = convert(datetime, '2023-07-06 01:03'), 
 @EndDt	date = convert(datetime, '2024-02-01')

/***************************************************************************************************
	Deliveries
	- Localization needed: FINANCIAL_CLASS categories
****************************************************************************************************/

drop table if exists #patients
select distinct
	PATIENT.PAT_ID
	,PATIENT.PAT_MRN_ID
	,OB_HSB_DELIVERY.DELIVERY_DATE_CSN as PAT_ENC_CSN_ID
	,min(EPISODE.OB_DEL_PREG_EPI_ID) as PREGNANCY_EPISODE
	,min(OB_HSB_DELIVERY.SUMMARY_BLOCK_ID) as DELIVERY_EPISODE_ID
	,min(datediff(yy, PATIENT.BIRTH_DATE, OB_HSB_DELIVERY.OB_DELIVERY_DATE) - 
		case when dateadd(yy, datediff(yy, PATIENT.BIRTH_DATE, OB_HSB_DELIVERY.OB_DELIVERY_DATE), PATIENT.BIRTH_DATE) > OB_HSB_DELIVERY.OB_DELIVERY_DATE then 1 else 0 end) as PAT_AGE
	,min(coalesce(BABY.BIRTH_DATE, OB_HSB_DELIVERY.OB_DEL_BIRTH_DTTM)) as OB_DELIVERY_DATE -- need to account for multiples
	,min(OB_HSB_DELIVERY.OB_HX_GEST_AGE) as GEST_AGE_DAYS_TOTAL
	,min(floor(OB_HSB_DELIVERY.OB_HX_GEST_AGE / 7)) as GEST_AGE_WEEKS
	,min(OB_HSB_DELIVERY.OB_HX_GEST_AGE % 7) as GEST_AGE_DAYS
	,min(BABY.PED_GEST_AGE) as PED_GEST_AGE
	,min(case when OB_HX_OUTCOME_C = 3 then 1 else 0 end) as PRETERM
	,min(case when CLARITY_EPM.FINANCIAL_CLASS in (
			'1' -- Commercial
			,'8' -- Group Health Plan
			,'10' -- Blue Shield
			,'13' -- DK Regional
			,'103' -- Blue Cross Out of State
			,'104' -- Exchange
			,'108' -- Blue Cross
			) then 'Private'
		when CLARITY_EPM.FINANCIAL_CLASS in (
			'2' -- Medicare
			,'3' -- Medicaid
			,'6' -- Tricare
			,'7' -- Champva
			,'11' -- Medigap
			,'100' -- TennCare
			,'102' -- Medicare Advantage
			,'105' -- Pending Medicaid
			,'106' -- Other Governmental
			,'109' -- Transplant
			) then 'Government'
		when CLARITY_EPM.FINANCIAL_CLASS in (
			'5' -- Worker's Comp
			,'9' -- FECA Black Lung
			,'12' -- Other
			,'101' -- Third Party Liability
			) then 'Other'
		when CLARITY_EPM.FINANCIAL_CLASS = '4' 
			then 'Uninsured' else null end) as INSURANCE
	,min(OB_DEL_BLOOD_LOSS) as OB_DEL_BLOOD_LOSS
	,min(PAT_ENC_HSP.HOSP_ADMSN_TIME) as HOSP_ADMSN_TIME
	,min(PAT_ENC_HSP.HOSP_DISCH_TIME) as HOSP_DISCH_TIME
into #patients
from PATIENT	
	inner join PAT_ENC_HSP
		on (PAT_ENC_HSP.PAT_ID = PATIENT.PAT_ID)
	inner join OB_HSB_DELIVERY
		on (OB_HSB_DELIVERY.DELIVERY_DATE_CSN = PAT_ENC_HSP.PAT_ENC_CSN_ID)
	inner join EPISODE	
		on (EPISODE.EPISODE_ID = OB_HSB_DELIVERY.SUMMARY_BLOCK_ID)
	left join HSP_ACCOUNT
		on (HSP_ACCOUNT.HSP_ACCOUNT_ID = PAT_ENC_HSP.HSP_ACCOUNT_ID)
	left join CLARITY_EPM
		on (CLARITY_EPM.PAYOR_ID = HSP_ACCOUNT.PRIMARY_PAYOR_ID)
	left join HSP_LD_MOM_CHILD
		on (HSP_LD_MOM_CHILD.PAT_ENC_CSN_ID = OB_HSB_DELIVERY.DELIVERY_DATE_CSN)
	left join PATIENT BABY
		on (BABY.PAT_ID = EPISODE.OB_DELIVERY_BABY_ID)
where OB_HSB_DELIVERY.OB_DEL_EPIS_TYPE_C = 10 -- Obstetrics - Delivery
	and OB_HSB_DELIVERY.OB_DELIVERY_DATE >= @StartDt and OB_HSB_DELIVERY.OB_DELIVERY_DATE < @EndDt
group by PATIENT.PAT_ID
	,PATIENT.PAT_MRN_ID
	,OB_HSB_DELIVERY.DELIVERY_DATE_CSN 


/***************************************************************************************************
	Delivery data
****************************************************************************************************/

drop table if exists #delivery
select
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,OB_HSB_DELIVERY.SUMMARY_BLOCK_ID as DELIVERY_EPISODE_ID
	,stuff((select ';' + ZC_OB_DEL_INDUCT.NAME 
			from ZC_OB_DEL_INDUCT
			inner join OB_HSB_DEL_INDUCT
				on (OB_HSB_DEL_INDUCT.OB_DEL_INDUCTION_C = ZC_OB_DEL_INDUCT.OBD_INDUCTION_C)
			where OB_HSB_DEL_INDUCT.SUMMARY_BLOCK_ID = OB_HSB_DELIVERY.SUMMARY_BLOCK_ID
			for xml path('')), 1, 1, '')
		as INDUCTION 
	,stuff((select ';' + ZC_OB_DEL_AUGMENT.NAME 
			from ZC_OB_DEL_AUGMENT
			inner join OB_HSB_DEL_AUGMENT
				on (OB_HSB_DEL_AUGMENT.OB_DEL_AUGMENT_C = ZC_OB_DEL_AUGMENT.OB_DEL_AUGMENT_C)
			where OB_HSB_DEL_AUGMENT.SUMMARY_BLOCK_ID = OB_HSB_DELIVERY.SUMMARY_BLOCK_ID
			for xml path('')), 1, 1, '') 
		as AUGMENTATION
	,ZC_DELIVERY_TYPE.NAME as DELIVERY_METHOD 
	,OB_DEL_1ST_STAGE_HR	
	,OB_DEL_1ST_STAGE_M	
	,OB_DEL_2ND_STAGE_HR	
	,OB_DEL_2ND_STAGE_M	
	,OB_DEL_3RD_STAGE_M
	,stuff((select ';' + ZC_BH_PLACENTA_REM.NAME 
			from ZC_BH_PLACENTA_REM
			inner join DELIVERY_PLCTA_RM
				on (DELIVERY_PLCTA_RM.DEL_PLACENTA_RM_C = ZC_BH_PLACENTA_REM.BH_PLACENTA_C)
			where DELIVERY_PLCTA_RM.SUMMARY_BLOCK_ID = OB_HSB_DELIVERY.SUMMARY_BLOCK_ID
			for xml path('')), 1, 1, '') as PLACENTA_REMOVAL
	,stuff((select ';' + ZC_BH_PRESENTATION.NAME 
			from ZC_BH_PRESENTATION
			inner join DELIVERY_PRES
				on (DELIVERY_PRES.DEL_PRESENTATION_C = ZC_BH_PRESENTATION.BH_PRESENTATION_C)
			where DELIVERY_PRES.SUMMARY_BLOCK_ID = OB_HSB_DELIVERY.SUMMARY_BLOCK_ID
			for xml path('')), 1, 1, '') as PRESENTATION
	,stuff((select ';' + ZC_OB_DEL_EPISIO.NAME 
			from ZC_OB_DEL_EPISIO
			inner join OB_HSB_DEL_EPISIO
				on (OB_HSB_DEL_EPISIO.OB_DEL_EPISIO_C = ZC_OB_DEL_EPISIO.OBD_EPISIOTOMY_C)
			where OB_HSB_DEL_EPISIO.SUMMARY_BLOCK_ID = OB_HSB_DELIVERY.SUMMARY_BLOCK_ID
			for xml path('')), 1, 1, '') as EPISIOTOMY
	,stuff((select ';' + ZC_OB_DEL_LACER.NAME 
			from ZC_OB_DEL_LACER
			inner join OB_HSB_DEL_LACERAT
				on (OB_HSB_DEL_LACERAT.OB_DEL_LACER_C = ZC_OB_DEL_LACER.OBD_LACERATIONS_C)
			where OB_HSB_DEL_LACERAT.SUMMARY_BLOCK_ID = OB_HSB_DELIVERY.SUMMARY_BLOCK_ID
			for xml path('')), 1, 1, '') as LACERATIONS
	,stuff((select ';' + ZC_OB_HX_ANESTH.NAME 
			from ZC_OB_HX_ANESTH
			inner join DELIVERY_ANES_MTHD
				on (DELIVERY_ANES_MTHD.DEL_ANESTH_METHOD_C = ZC_OB_HX_ANESTH.OB_HX_ANESTH_C)
			where DELIVERY_ANES_MTHD.SUMMARY_BLOCK_ID = OB_HSB_DELIVERY.SUMMARY_BLOCK_ID
			for xml path('')), 1, 1, '') as ANESTHESIA_TYPE
into #delivery 
from #patients
	inner join OB_HSB_DELIVERY
		on (OB_HSB_DELIVERY.DELIVERY_DATE_CSN = #patients.PAT_ENC_CSN_ID)
	inner join EPISODE	
		on (EPISODE.EPISODE_ID = OB_HSB_DELIVERY.SUMMARY_BLOCK_ID)
	left join ZC_DELIVERY_TYPE
		on (ZC_DELIVERY_TYPE.DELIVERY_TYPE_C = OB_HSB_DELIVERY.OB_DEL_DELIV_METH_C)
where OB_HSB_DELIVERY.OB_DEL_EPIS_TYPE_C = 10
order by #patients.PAT_ENC_CSN_ID


/***************************************************************************************************
	Prior admissions
****************************************************************************************************/

drop table if exists #admissions
select
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID as DELIVERY_CSN
	,PAT_ENC_HSP.PAT_ENC_CSN_ID
	,PAT_ENC_HSP.HOSP_ADMSN_TIME
into #admissions
from #patients
	inner join EPISODE_LINK
		on ((EPISODE_LINK.EPISODE_ID = #patients.PREGNANCY_EPISODE) -- linked to the pregnancy episode
		 and (EPISODE_LINK.PAT_ENC_CSN_ID != #patients.PAT_ENC_CSN_ID)) -- not the delivery CSN
	inner join PAT_ENC_HSP
		on (PAT_ENC_HSP.PAT_ENC_CSN_ID = EPISODE_LINK.PAT_ENC_CSN_ID)
where PAT_ENC_HSP.ADT_PAT_CLASS_C in ('101', '104') -- Inpatient, 
	and PAT_ENC_HSP.ADT_PATIENT_STAT_C != 6
	and PAT_ENC_HSP.HOSP_ADMSN_TIME < #patients.HOSP_ADMSN_TIME -- prior to delivery admission


/***************************************************************************************************
	Blood loss
	- Localization needed: FLO_MEAS_IDs
****************************************************************************************************/

drop table if exists #blood_loss
select
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,sum(case when V_OB_BLOOD_LOSS.FLO_MEAS_ID = '1020001201' -- R VU INTRAUTERINE TAMPONADE OUTPUT
		then convert(numeric, V_OB_BLOOD_LOSS.MEAS_VALUE) else 0 end) as IU_TAMPONADE_OUTPUT
	,sum(case when V_OB_BLOOD_LOSS.FLO_MEAS_ID = '1020006075' -- R VTHH TOTAL BLOOD LOSS
		then convert(numeric, V_OB_BLOOD_LOSS.MEAS_VALUE) else 0 end) as VTHH_TBL
	,sum(case when V_OB_BLOOD_LOSS.FLO_MEAS_ID = '1028605' -- R BC TOTAL BLOOD LOSS
		then convert(numeric, V_OB_BLOOD_LOSS.MEAS_VALUE) else 0 end) as BC_TBL
	,sum(case when V_OB_BLOOD_LOSS.FLO_MEAS_ID = '1221' -- R ESTIMATED BLOOD LOSS
		then convert(numeric, V_OB_BLOOD_LOSS.MEAS_VALUE) else 0 end) as EBL
	,sum(case when V_OB_BLOOD_LOSS.FLO_MEAS_ID = '25204' -- R ONCBCN TP VOLUME REMOVED
		then convert(numeric, V_OB_BLOOD_LOSS.MEAS_VALUE) else 0 end) as ONCBCN_TP_VOL_RM
	,sum(case when V_OB_BLOOD_LOSS.FLO_MEAS_ID = '8605' -- R TOTAL BLOOD LOSS
		then convert(numeric, V_OB_BLOOD_LOSS.MEAS_VALUE) else 0 end) as TBL
into #blood_loss
from #patients
	 inner join V_OB_BLOOD_LOSS
		on (V_OB_BLOOD_LOSS.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
group by #patients.PAT_ID, #patients.PAT_ENC_CSN_ID


/***************************************************************************************************
	Social history
****************************************************************************************************/

drop table if exists #social_hx
select
	PAT_ID
	,PAT_ENC_CSN_ID
	,MARITAL_STATUS
	,EDUCATION_LEVEL
	,TOBACCO_USE
	,DRUG_USE
into #social_hx
from (
	select 
		#patients.PAT_ID
		,#patients.PAT_ENC_CSN_ID
		,ZC_MARITAL_STATUS.NAME as MARITAL_STATUS
		,ZC_EDU_LEVEL.NAME as EDUCATION_LEVEL
		,ZC_SMOKING_TOB_USE.NAME as TOBACCO_USE
		,DRUG_USE.NAME as DRUG_USE
		,row_number() over (partition by #patients.PAT_ID order by SOCIAL_HX.CONTACT_DATE) as ROW_NUM
	from #patients
		inner join PATIENT
			on (PATIENT.PAT_ID = #patients.PAT_ID)
		left join ZC_MARITAL_STATUS
			on (ZC_MARITAL_STATUS.MARITAL_STATUS_C = PATIENT.MARITAL_STATUS_C)
		left join SOCIAL_HX
			on ((SOCIAL_HX.PAT_ID = PATIENT.PAT_ID)
				and (SOCIAL_HX.CONTACT_DATE <= #patients.OB_DELIVERY_DATE))
		left join ZC_EDU_LEVEL
			on (ZC_EDU_LEVEL.EDU_LEVEL_C = SOCIAL_HX.EDU_LEVEL_C)
		left join ZC_SMOKING_TOB_USE
			on (ZC_SMOKING_TOB_USE.SMOKING_TOB_USE_C = SOCIAL_HX.SMOKING_TOB_USE_C)
		left join ZC_ALCOHOL_USE DRUG_USE
			on (DRUG_USE.ALCOHOL_USE_C = SOCIAL_HX.ILL_DRUG_USER_C)
) as SOCIAL_HX
where ROW_NUM = 1


/***************************************************************************************************
	Race and ethnicity
	- Localization needed: PATIENT_RACE_C, ETHNIC_GROUP_C
****************************************************************************************************/

drop table if exists #race_ethnicity
select distinct
	PAT_ID
	,RACE
	,ETHNICITY
into #race_ethnicity
from (
	select 
		PAT_ID
		,max('Multiple') RACE
		,max(ETHNICITY) ETHNICITY
	from (
		select distinct 
			#patients.PAT_ID
			,case when PATIENT_RACE.PATIENT_RACE_C = 1 -- White
					then 'White' 
				when PATIENT_RACE.PATIENT_RACE_C = 2 -- Black or African American
					then 'Black'
				when PATIENT_RACE.PATIENT_RACE_C in (
					4 -- Other Asian
					,9 -- Chinese
					,10 -- Filipino
					,11 -- Japanese
					,12 -- Korean
					,13 -- Vietnamese
					,14 -- Asian Indian
					,19 -- Asian
					) then 'Asian'
				when PATIENT_RACE.PATIENT_RACE_C = 3 -- American Indian or Alaska Native
					then 'American Indian'
				when PATIENT_RACE.PATIENT_RACE_C in (
					5 -- Native Hawaiian or Other Pacific Islander
					,15 -- Native Hawaiian
					,16 -- Guamanian or Chamorro
					,17 -- Samoan
					,18 -- Other Pacific Islander
					,20 -- Native Hawaiian or Other Pacific Islander
					) then 'Native Hawaiian' 
					else NULL end RACE
			,case when PATIENT.ETHNIC_GROUP_C = 1 then 'Not Hispanic' when PATIENT.ETHNIC_GROUP_C in (
				2
				,5
				,6
				,7
				,8
				) then 'Hispanic' else NULL end ETHNICITY
		from #patients 
			inner join PATIENT
				on (PATIENT.PAT_ID = #patients.PAT_ID)
			left join PATIENT_RACE 
				on (PATIENT_RACE.PAT_ID = PATIENT.PAT_ID)
	) as ee
	group by PAT_ID
	having count(distinct RACE) > 1
	union 
	select 
		PAT_ID
		,max(RACE) RACE
		, max(ETHNICITY) ETHNICITY
	from (
		select distinct 
			PATIENT.PAT_ID
			,case when PATIENT_RACE.PATIENT_RACE_C = 1 -- White
					then 'White' 
				when PATIENT_RACE.PATIENT_RACE_C = 2 -- Black or African American
					then 'Black'
				when PATIENT_RACE.PATIENT_RACE_C in (
					4 -- Other Asian
					,9 -- Chinese
					,10 -- Filipino
					,11 -- Japanese
					,12 -- Korean
					,13 -- Vietnamese
					,14 -- Asian Indian
					,19 -- Asian
					) then 'Asian'
				when PATIENT_RACE.PATIENT_RACE_C = 3 -- American Indian or Alaska Native
					then 'American Indian'
				when PATIENT_RACE.PATIENT_RACE_C in (
					5 -- Native Hawaiian or Other Pacific Islander
					,15 -- Native Hawaiian
					,16 -- Guamanian or Chamorro
					,17 -- Samoan
					,18 -- Other Pacific Islander
					,20 -- Native Hawaiian or Other Pacific Islander
					) then 'Native Hawaiian' 
					else NULL end RACE
			,case when PATIENT.ETHNIC_GROUP_C = 1 then 'Not Hispanic' when PATIENT.ETHNIC_GROUP_C in (
				2
				,5
				,6
				,7
				,8
				) then 'Hispanic' else NULL end ETHNICITY
		from #patients 
			inner join PATIENT
				on (PATIENT.PAT_ID = #patients.PAT_ID)
			left join PATIENT_RACE 
				on (PATIENT_RACE.PAT_ID = PATIENT.PAT_ID)
	) as ee
	group by PAT_ID
	having count(distinct RACE) <= 1
) as e


/***************************************************************************************************
	OB history
****************************************************************************************************/

drop table if exists #ob_hx
select 
	PAT_ID
	,PAT_ENC_CSN_ID
	,OB_DELIVERY_DATE
	,CONTACT_DATE
	,OB_GRAVIDITY
	,OB_PARITY
	,OB_PREMATURE
into #ob_hx
from (
	select	
		#patients.PAT_ID
		,#patients.PAT_ENC_CSN_ID
		,#patients.OB_DELIVERY_DATE
		,OB_HX_HSB.CONTACT_DATE
		,OB_TOTAL.OB_GRAVIDITY
		,coalesce(OB_TOTAL.OB_PARITY, 0) - 1 as OB_PARITY
		,coalesce(OB_TOTAL.OB_PREMATURE, 0) - #patients.PRETERM as OB_PREMATURE 
		,row_number() over (partition by #patients.PAT_ID order by OB_HX_HSB.CONTACT_DATE desc) as ROW_NUM
	from #patients
		left join OB_HX_HSB
			on ((OB_HX_HSB.PAT_ID = #patients.PAT_ID)	
				and (OB_HX_HSB.HX_LINK_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID))
		left join OB_TOTAL	
			on (OB_TOTAL.PAT_ENC_CSN_ID = OB_HX_HSB.PAT_ENC_CSN_ID)
) as OBH
where ROW_NUM = 1


/***************************************************************************************************
	Prior surgeries (Cesarean)
	- Based on CPT codes
****************************************************************************************************/

drop table if exists #prior_proc
select
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,coalesce(PRIOR_CD_CPT, 0) as PRIOR_CD_CPT
into #prior_proc
from #patients
	left join (
		select
			#patients.PAT_ID
			,#patients.PAT_ENC_CSN_ID
			,count(distinct OR_LOG.LOG_ID) as PRIOR_CD_CPT
		from #patients
			inner join OR_LOG
				on (OR_LOG.PAT_ID = #patients.PAT_ID)
			inner join F_LOG_BASED			-- helpful table about surgeries (times, people), one row per surgery log
				on ((F_LOG_BASED.LOG_ID = OR_LOG.LOG_ID)
					and (F_LOG_BASED.PROCEDURE_DATE <= #patients.OB_DELIVERY_DATE))
			inner join OR_LOG_ALL_PROC		-- get all procedures performed during a surgery log
				on (OR_LOG_ALL_PROC.LOG_ID = OR_LOG.LOG_ID)
			inner join OR_PROC_CPT_ID
				on (OR_PROC_CPT_ID.OR_PROC_ID = OR_LOG_ALL_PROC.OR_PROC_ID)
			inner join CLARITY_EAP
				on ((CLARITY_EAP.PROC_ID = OR_PROC_CPT_ID.CPT_ID)
					and (CLARITY_EAP.PROC_CODE in ('59514', '59515', '59620'))) -- CPT codes
		group by #patients.PAT_ID, #patients.PAT_ENC_CSN_ID
	) PP 
		on (PP.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)

		
/***************************************************************************************************
	Prior diagnoses
	- Based on ICD-10 codes
	- Problem list entries and encounter diagnoses entered prior to delivery
****************************************************************************************************/

drop table if exists #diagnoses
select
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,max(case when CODE like 'O72.%' then 1 else 0 end) as PRIOR_PPH
	,max(case when CODE = 'O72.1' then 1 else 0 end) as PRIOR_ATONIC_PPH
	,max(case when CODE in ('I10', 'O10', 'O11') or CODE like 'I10.%' or CODE like 'O10.%' or CODE like 'O11.%' then 1 else 0 end) as CHRONIC_HYPERTENSION
	,max(case when CODE = 'O13' or CODE like 'O13.%' then 1 else 0 end) as GEST_HYPERTENSION
	,max(case when CODE in ('O11', 'O14.0', 'O14.9') or CODE like 'O11.%' or CODE like 'O14.0%' or CODE like 'O14.9%' then 1 else 0 end) as PREE_NOSF
	,max(case when CODE in ('O14.1', 'O14.2') or CODE like 'O14.1%' or CODE like 'O14.2%' then 1 else 0 end) as PREE_SF
	,max(case when CODE = 'O15' or CODE like 'O15.%' then 1 else 0 end) as ECLAMPSIA
	,max(case when CODE = 'O14.2' or CODE like 'O14.2%' then 1 else 0 end) as HELLP
	,max(case when CODE in ('E11.9', 'O24.0', 'O24.1', 'O24.3', 'O24.8') then 1 else 0 end) as CHRONIC_DM
	,max(case when CODE = 'O24.4' or CODE like 'O24.4%' then 1 else 0 end) as GEST_DM
	,max(case when CODE in ('D50', 'D51', 'D52', 'D53', 'D55', 'D56', 'D57', 'D58', 'O99.0')
		or CODE like 'D50.%' or CODE like 'D51.%' or CODE like 'D52.%' or CODE like 'D53.%'
		or CODE like 'D55.%' or CODE like 'D56.%' or CODE like 'D57.%' or CODE like 'D58.%'
		or CODE like 'D59.%' then 1 else 0 end) as ANEMIA
	,max(case when CODE like 'E66.%' or CODE = 'O99.21' then 1 else 0 end) as OBESITY
	,max(case when CODE = 'D25' or CODE like 'D25.%' then 1 else 0 end) as LEIOMYOMAS
	,max(case when CODE = 'O40' or CODE like 'O40.%' then 1 else 0 end) as POLYHYDRAMNIOS
	,max(case when CODE = 'O30' or CODE like 'O30.%' then 1 else 0 end) as MULTIPLE_GEST
	,max(case when CODE = 'O44' or CODE like 'O44.%' then 1 else 0 end) as PLACENTA_PREVIA
	,max(case when CODE = 'O45' or CODe like 'O45.%' then 1 else 0 end) as PLACENTAL_ABRUPTION
	,max(case when CODE = 'O43.21' or CODE like 'O43.21%' then 1 else 0 end) as PLACENTA_ACCRETA
	,max(case when CODE = 'O43.22' or CODE like 'O43.22%' then 1 else 0 end) as PLACENTA_INCRETA
	,max(case when CODE = 'O43.23' or CODE like 'O43.23%' then 1 else 0 end) as PLACENTA_PERCRETA
	,max(case when CODE like 'O44.1%' or CODE like 'O44.3%' or CODE like 'O44.5%' or CODE like 'O45.%' or CODE like 'O46.%' then 1 else 0 end) as ANTEPARTUM_HEMORRHAGE
	,max(case when CODE like 'O36.4%' then 1 else 0 end) as FETAL_DEMISE
	,max(case when CODE like 'O41.12%' then 1 else 0 end) as CHORIOAMNIONITIS
	,max(case when CODE like 'O09.81%' then 1 else 0 end) as ART
	,max(case when CODE like 'O36.5%' then 1 else 0 end) as SMALL_FOR_GEST_AGE
	,max(case when CODE like 'O36.6%' then 1 else 0 end) as LARGE_FOR_GEST_AGE
	,max(case when CODE = 'G40' or CODE like 'G40.%' then 1 else 0 end) as SEIZURE_DISORDER
	,max(case when CODE in ('O99.33', 'Z72.0') or CODE like 'O99.33%' or CODE like 'Z72.0%' then 1 else 0 end) as TOBACCO_USE
	,max(case when CODE in ('O99.32', 'F19') or CODE like 'O99.32%' or CODE like 'F19.%' then 1 else 0 end) as DRUG_USE
	,max(case when CODE in ('I05', 'I06', 'I07', 'I08', 'I09', 'I20', 'I21', 'I22', 'I23', 'I24', 'I25', 'I30'
			,'I40', 'I41', 'I42', 'I43', 'I44', 'I45', 'I46', 'I47', 'I48', 'I49', 'I50', 'I51', 'I5')
		or CODE like 'I05.%' or CODE like 'I06.%' or CODE like 'I07.%' or CODE like 'I08.%' or CODE like 'I09.%' 
		or CODE like 'I20.%' or CODE like 'I21.%' or CODE like 'I22.%' or CODE like 'I23.%' or CODE like 'I24.%' 
		or CODE like 'I25.%' or CODE like 'I30.%' or CODE like 'I40.%' or CODE like 'I41.%' or CODE like 'I42.%' 
		or CODE like 'I43.%' or CODE like 'I44.%' or CODE like 'I45.%' or CODE like 'I46.%' or CODE like 'I47.%' 
		or CODE like 'I48.%' or CODE like 'I49.%' or CODE like 'I50.%' or CODE like 'I51.%' or CODE like 'I5.%' then 1 else 0 end) as HEART_DISEASE
	,max(case when CODE in ('E00', 'E01', 'E02', 'E03', 'E04', 'E05', 'E06', 'E07')
		or CODE like 'E00.%' or CODE like 'E01.%' or CODE like 'E02.%' or CODE like 'E03.%' or CODE like 'E04.%'
		or CODE like 'E05.%' or CODE like 'E06.%' or CODE like 'E07.%' then 1 else 0 end) as THYROID_DISEASE
	,max(case when CODE in ('N17', 'N18', 'N19') or CODE like 'N17.%' or CODE like 'N18.%' or CODE like 'N19.%' then 1 else 0 end) as RENAL_DISEASE
	,max(case when CODE = 'J45' or CODE like 'J45.%' then 1 else 0 end) as ASTHMA
	,max(case when CODE in ('F32', 'F33') or CODE like 'F32.%' or CODE like 'F33.%' then 1 else 0 end) as DEPRESSION 
	,max(case when CODE in ('K90', 'K91', 'K92', 'K93', 'K94', 'O99.6') or CODE like 'K90.%' 
		or CODE like 'K91.%' or CODE like 'K92.%' or CODE like 'K93.%' or CODE like 'K94.%' then 1 else 0 end) as GI_DISEASE 
	,max(case when CODE in ('D69.3', 'D69.4', 'D69.5', 'D69.6') or CODE like 'D69.4%' or CODE like 'D69.5%' or CODE = 'D69.6'then 1 else 0 end) as THROMBOCYTOPENIA
	,max(case when CODE in ('D65', 'D66', 'D67', 'D68', 'D69', 'O72.3') or CODE like 'D65.%' or CODE like 'D66.%'
		or CODE like 'D67.%' or CODE like 'D68.%' or CODE like 'D69.%' or CODE = 'O72.3'then 1 else 0 end) as COAGULOPATHY 
	,max(case when CODE like 'O32.1%' then 1 else 0 end) as BREECH
	,max(case when CODE = 'O42' or CODE like 'O42.%' then 1 else 0 end) as PROM
	,max(case when CODE like 'O99.82%' then 1 else 0 end) as GBS
into #diagnoses
from #patients
	inner join (
		select
			#patients.PAT_ID
			,PROBLEM_LIST.DATE_OF_ENTRY as START_DT
			,ICD10.CODE
		from #patients
			inner join PROBLEM_LIST
				on (PROBLEM_LIST.PAT_ID = #patients.PAT_ID)
			inner join EDG_CURRENT_ICD10 ICD10
				on (ICD10.DX_ID = PROBLEM_LIST.DX_ID)
	union
		select
			#patients.PAT_ID
			,PAT_ENC.CONTACT_DATE as START_DT
			,ICD10.CODE
		from #patients
			inner join PAT_ENC
				on (PAT_ENC.PAT_ID = #patients.PAT_ID)
			inner join PAT_ENC_DX
				on (PAT_ENC_DX.PAT_ENC_CSN_ID = PAT_ENC.PAT_ENC_CSN_ID)
			inner join EDG_CURRENT_ICD10 ICD10
				on (ICD10.DX_ID = PAT_ENC_DX.DX_ID)
	) as PRIOR_DX
		on ((PRIOR_DX.PAT_ID = #patients.PAT_ID)
			and (PRIOR_DX.START_DT < #patients.OB_DELIVERY_DATE))
group by #patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID


/***************************************************************************************************
	Admission vitals, prior to delivery
	- Localization needed: FLO_MEAS_ID
****************************************************************************************************/

drop table if exists #vitals
select
	PAT_ID
	,PAT_ENC_CSN_ID
	,min(case when FLO_MEAS_ID = '11' and ROW_NUM = 1 then MEAS_VALUE else null end) as FIRST_HEIGHT -- HEIGHT
	,min(case when FLO_MEAS_ID = '14' and ROW_NUM = 1 then convert(numeric, MEAS_VALUE) * 0.0283495 else null end) as FIRST_WEIGHT -- WEIGHT/SCALE
	,min(case when FLO_MEAS_ID = '301070' and ROW_NUM = 1 then MEAS_VALUE else null end) as FIRST_BMI -- R BMI
	,min(case when FLO_MEAS_ID = '5' and ROW_NUM = 1 then left(MEAS_VALUE, charindex('/', MEAS_VALUE) - 1) else null end) as FIRST_SBP -- BLOOD PRESSURE
	,max(case when FLO_MEAS_ID = '5' then convert(int, left(MEAS_VALUE, charindex('/', MEAS_VALUE) - 1)) else null end) as MAX_SBP -- BLOOD PRESSURE
	,min(case when FLO_MEAS_ID = '5' then convert(int, left(MEAS_VALUE, charindex('/', MEAS_VALUE) - 1)) else null end) as MIN_SBP -- BLOOD PRESSURE
	,min(case when FLO_MEAS_ID = '5' and ROW_NUM = 1 then right(MEAS_VALUE, len(MEAS_VALUE) - charindex('/', MEAS_VALUE)) else null end) as FIRST_DBP -- BLOOD PRESSURE
	,max(case when FLO_MEAS_ID = '5' then convert(int, right(MEAS_VALUE, len(MEAS_VALUE) - charindex('/', MEAS_VALUE))) else null end) as MAX_DBP -- BLOOD PRESSURE
	,min(case when FLO_MEAS_ID = '5' then convert(int, right(MEAS_VALUE, len(MEAS_VALUE) - charindex('/', MEAS_VALUE))) else null end) as IN_DBP -- BLOOD PRESSURE
	,min(case when FLO_MEAS_ID = '8' and ROW_NUM = 1 then MEAS_VALUE else null end) as FIRST_PULSE -- PULSE
	,max(case when FLO_MEAS_ID = '8' then convert(numeric, MEAS_VALUE) else null end) as MAX_PULSE -- PULSE
	,min(case when FLO_MEAS_ID = '8'then convert(numeric, MEAS_VALUE) else null end) as MIN_PULSE -- PULSE
	,min(case when FLO_MEAS_ID = '301240' and ROW_NUM = 1 then MEAS_VALUE else null end) as FIRST_HR -- R HEART RATE MONITOR
	,max(case when FLO_MEAS_ID = '301240' then convert(numeric, MEAS_VALUE) else null end) as MAX_HR -- R HEART RATE MONITOR
	,min(case when FLO_MEAS_ID = '301240' then convert(numeric, MEAS_VALUE) else null end) as MIN_HR -- R HEART RATE MONITOR
	,min(case when FLO_MEAS_ID = '9' and ROW_NUM = 1 then MEAS_VALUE else null end) as FIRST_RR -- RESPIRATIONS
	,max(case when FLO_MEAS_ID = '9' then convert(numeric, MEAS_VALUE) else null end) as MAX_RR -- RESPIRATIONS
	,min(case when FLO_MEAS_ID = '9'then convert(numeric, MEAS_VALUE) else null end) as MIN_RR -- RESPIRATIONS
	,min(case when FLO_MEAS_ID = '10' and ROW_NUM = 1 then MEAS_VALUE else null end) as FIRST_O2SAT -- PULSE OXIMETRY
	,max(case when FLO_MEAS_ID = '10' then convert(numeric, MEAS_VALUE) else null end) as MAX_O2SAT -- PULSE OXIMETRY
	,min(case when FLO_MEAS_ID = '10'then convert(numeric, MEAS_VALUE) else null end) as MIN_O2SAT -- PULSE OXIMETRY
	,min(case when FLO_MEAS_ID = '6' and ROW_NUM = 1 then MEAS_VALUE else null end) as FIRST_TEMP -- TEMPERATURE
	,max(case when FLO_MEAS_ID = '6' then convert(numeric, MEAS_VALUE) else null end) as MAX_TEMP -- TEMPERATURE
	,min(case when FLO_MEAS_ID = '6'then convert(numeric, MEAS_VALUE) else null end) as MIN_TEMP -- TEMPERATURE
	,min(case when FLO_MEAS_ID = '12012' and REV_ROW_NUM = 1 then MEAS_VALUE else null end) as FETAL_HEART_RATE -- R OB FHR DOPPLER/FETOSCOPE RATE
	,min(case when FLO_MEAS_ID = '12301' and REV_ROW_NUM = 1 then MEAS_VALUE else null end) as DTR_RUE -- R OB RUE
	,min(case when FLO_MEAS_ID = '12302' and REV_ROW_NUM = 1 then MEAS_VALUE else null end) as DTR_LUE -- R OB LUE
	,min(case when FLO_MEAS_ID = '12303' and REV_ROW_NUM = 1 then MEAS_VALUE else null end) as DTR_RLE -- R OB RLE
	,min(case when FLO_MEAS_ID = '12305' and REV_ROW_NUM = 1 then MEAS_VALUE else null end) as DTR_RLE -- R OB LLE
	,min(case when FLO_MEAS_ID = '304005151' and REV_ROW_NUM = 1 then MEAS_VALUE else null end) as RESPIRATORY_SUPPORT -- R VUMC RESP SUPPORT TYPE
	,min(case when FLO_MEAS_ID = '7922' and REV_ROW_NUM = 1 then MEAS_VALUE else null end) as FLOW_RATE -- R VUMC OXYGEN FLOW DELIVERED
into #vitals
from (
	select
		#patients.PAT_ID
		,#patients.PAT_ENC_CSN_ID
		,#patients.OB_DELIVERY_DATE
		,FSD.FLO_MEAS_ID
		,FSD.MEAS_VALUE
		,FSD.ENTRY_TIME
		,row_number() over (partition by #patients.PAT_ENC_CSN_ID, FSD.FLO_MEAS_ID order by FSD.ENTRY_TIME) as ROW_NUM
		,row_number() over (partition by #patients.PAT_ENC_CSN_ID, FSD.FLO_MEAS_ID order by FSD.ENTRY_TIME desc) as REV_ROW_NUM
	from #patients
		inner join IP_DATA_STORE IPDS
			on (IPDS.EPT_CSN = #patients.PAT_ENC_CSN_ID)
		inner join IP_FLWSHT_REC INP
			on (INP.INPATIENT_DATA_ID = IPDS.INPATIENT_DATA_ID)
		inner join IP_FLWSHT_MEAS FSD
			on (FSD.FSD_ID = INP.FSD_ID)
	where FSD.FLO_MEAS_ID in (
		'11' -- HEIGHT
		,'14' -- WEIGHT/SCALE
		,'301070' -- R BMI
		,'5' -- BLOOD PRESSURE
		,'8' -- PULSE
		,'301240' -- R HEART RATE MONITOR
		,'9' -- RESPIRATIONS
		,'10' -- PULSE OXIMETRY
		,'6' -- TEMPERATURE
		,'12012' -- R OB FHR DOPPLER/FETOSCOPE RATE
		,'12301' -- R OB RUE
		,'12302' -- R OB LUE
		,'12303' -- R OB RLE
		,'12305' -- R OB LLE
		,'304005151' -- R VUMC RESP SUPPORT TYPE
		,'7922' -- R VUMC OXYGEN FLOW DELIVERED
	)
	and FSD.ENTRY_TIME <= #patients.OB_DELIVERY_DATE
) as V
group by PAT_ID
	,PAT_ENC_CSN_ID


/***************************************************************************************************
	Pre-admission vitals (prior to delivery)
	- Localization needed: FLO_MEAS_IDs
****************************************************************************************************/

drop table if exists #preadmit_vitals
select
	PAT_ID
	,PAT_ENC_CSN_ID
	,min(case when FLO_MEAS_ID = '11' and ROW_NUM = 1 then MEAS_VALUE else null end) as FIRST_HEIGHT -- HEIGHT
	,min(case when FLO_MEAS_ID = '14' and ROW_NUM = 1 and datediff(month, ENTRY_TIME, HOSP_ADMSN_TIME) <= 1 -- WEIGHT/SCALE
		then convert(numeric, MEAS_VALUE) * 0.0283495 else null end) as FIRST_WEIGHT
	,min(case when FLO_MEAS_ID = '301070' and ROW_NUM = 1 and datediff(month, ENTRY_TIME, HOSP_ADMSN_TIME) <= 1 -- R BMI
		then MEAS_VALUE else null end) as FIRST_BMI
into #preadmit_vitals
from (
	select
		#patients.PAT_ID
		,#patients.PAT_ENC_CSN_ID
		,#patients.HOSP_ADMSN_TIME
		,FSD.FLO_MEAS_ID
		,FSD.MEAS_VALUE
		,FSD.ENTRY_TIME
		,row_number() over (partition by #patients.PAT_ENC_CSN_ID, FSD.FLO_MEAS_ID order by FSD.ENTRY_TIME desc) as ROW_NUM
	from #patients
		inner join IP_FLWSHT_REC INP
			on (INP.PAT_ID = #patients.PAT_ID)
		inner join IP_FLWSHT_MEAS FSD
			on (FSD.FSD_ID = INP.FSD_ID)
	where FSD.FLO_MEAS_ID in (
		'11' -- HEIGHT
		,'14' -- WEIGHT/SCALE
		,'301070' -- R BMI
	)
	and FSD.ENTRY_TIME < #patients.HOSP_ADMSN_TIME
) as V
group by PAT_ID
	,PAT_ENC_CSN_ID


/***************************************************************************************************
	Nursing checklist flowsheets
	- Localization needed: FLO_MEAS_IDs, MEAS_VALUEs
****************************************************************************************************/

drop table if exists #checklist
select
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,max(case when FSD.FLO_MEAS_ID = '1020100679' and FSD.MEAS_VALUE = 'First degree relative with history of postpartum hemorrhage' then 1 else 0 end) as FAMILY_HX_PPH -- R OB PPH RISK HISTORY OF PPH
	,max(case when FSD.FLO_MEAS_ID = '1020100677' then FSD.MEAS_VALUE else null end) as PRIOR_VD -- R OB PPH RISK PREVIOUS VAGINAL BIRTH
	,max(case when FSD.FLO_MEAS_ID = '1020100679' then -- R OB PPH RISK HISTORY OF PPH
		case when FSD.MEAS_VALUE = 'History of one postpartum hemorrhage' then 1
			when FSD.MEAS_VALUE = 'History of more than one postpartum hemorrhage' then 2
			else 0 end
		else null end) as PRIOR_PPH 
	,max(case when FSD.FLO_MEAS_ID = '1020100676' and FSD.MEAS_VALUE = 'Prior cesarean birth' then 1 else 0 end) as PRIOR_CD -- R OB PPH RISK PRIOR CESAREAN OR UTERINE INCISION
	,max(case when FSD.FLO_MEAS_ID = '1020100676' and FSD.MEAS_VALUE = 'Prior uterine incision' then 1 else 0 end) as PRIOR_UTERINE_INCISION -- R OB PPH RISK PRIOR CESAREAN OR UTERINE INCISION
	,max(case when FSD.FLO_MEAS_ID = '1020100682' then FSD.MEAS_VALUE else null end) as FIBROIDS -- R OB PPH RISK UTERINE FIBROIDS
	,max(case when FSD.FLO_MEAS_ID = '1020100685' then FSD.MEAS_VALUE else null end) as POLYHYDRAMNIOS -- R OB PPH RISK POLYHYDRAMNIOS
	,max(case when FSD.FLO_MEAS_ID = '1020100688' then -- R OB PPH RISK PLACENTA PREVIA
		case when FSD.MEAS_VALUE = 'Placenta previa' then 1
			when FSD.MEAS_VALUE = 'Low lying placenta' then 2 
			else 0 end
		else null end) as PLACENTA_PREVIA
	,max(case when FSD.FLO_MEAS_ID = '1020100686' then FSD.MEAS_VALUE else null end) as ACTIVE_BLEEDING -- R OB PPH RISK ACTIVE BLEEDING
	,max(case when FSD.FLO_MEAS_ID = '1020100691' then FSD.MEAS_VALUE else null end) as PLACENTAL_ABRUPTION -- R OB PPH RISK SUSPECTED ABRUPTION
	,max(case when FSD.FLO_MEAS_ID = '1020100687' and FSD.MEAS_VALUE like '%Placenta accreta%' then 1 else 0 end) as PLACENTA_ACCRETA -- R OB PPH RISK SUSPECTED PLACENTA ACCRETA OR PERCRETA
	,max(case when FSD.FLO_MEAS_ID = '1020100687' and FSD.MEAS_VALUE like '%Placenta percreta%' then 1 else 0 end) as PLACENTA_PERCRETA -- R OB PPH RISK SUSPECTED PLACENTA ACCRETA OR PERCRETA
	,max(case when FSD.FLO_MEAS_ID = '1020100831' then FSD.MEAS_VALUE else null end) as FETAL_DEMISE -- R OB PPH RISK KNOWN FETAL DEMISE
	,max(case when FSD.FLO_MEAS_ID = '1020100683' then FSD.MEAS_VALUE else null end) as CHORIO -- R OB PPH RISK CHORIOAMNIONITIS
	,max(case when FSD.FLO_MEAS_ID = '1020100829' then FSD.MEAS_VALUE else null end) as INDUCTION -- R OB PPH INDUCTION OF LABOR OR CERVICAL RIPENING
	,max(case when FSD.FLO_MEAS_ID = '1020100689' then FSD.MEAS_VALUE else null end) as PROLONGED -- R OB PPH RISK LABOR GREATER THAN 18 HOURS
	,max(case when FSD.FLO_MEAS_ID = '1020100690' then FSD.MEAS_VALUE else null end) as PROLONGED_2ND -- R OB PPH RISK PROLONGED 2ND STAGE
	,max(case when FSD.FLO_MEAS_ID = '1020100678' then FSD.MEAS_VALUE else null end) as BLEEDING_DISORDER -- R OB PPH RISK BLEEDING DISORDER
into #checklist
from #patients
	inner join IP_DATA_STORE IPDS
		on (IPDS.EPT_CSN = #patients.PAT_ENC_CSN_ID)
	inner join IP_FLWSHT_REC INP
		on (INP.INPATIENT_DATA_ID = IPDS.INPATIENT_DATA_ID)
	inner join IP_FLWSHT_MEAS FSD
		on (FSD.FSD_ID = INP.FSD_ID)
where FSD.FLO_MEAS_ID in (
	'1020100679' -- R OB PPH RISK HISTORY OF PPH
	,'1020100677' -- R OB PPH RISK PREVIOUS VAGINAL BIRTH
	,'1020100676' -- R OB PPH RISK PRIOR CESAREAN OR UTERINE INCISION
	,'1020100682' -- R OB PPH RISK UTERINE FIBROIDS
	,'1020100685' -- R OB PPH RISK POLYHYDRAMNIOS
	,'1020100688' -- R OB PPH RISK PLACENTA PREVIA
	,'1020100686' -- R OB PPH RISK ACTIVE BLEEDING
	,'1020100687' -- R OB PPH RISK SUSPECTED PLACENTA ACCRETA OR PERCRETA
	,'1020100831' -- R OB PPH RISK KNOWN FETAL DEMISE
	,'1020100683' -- R OB PPH RISK CHORIOAMNIONITIS
	,'1020100829' -- R OB PPH INDUCTION OF LABOR OR CERVICAL RIPENING
	,'1020100689' -- R OB PPH RISK LABOR GREATER THAN 18 HOURS
	,'1020100690' -- R OB PPH RISK PROLONGED 2ND STAGE
	,'1020100678' -- R OB PPH RISK BLEEDING DISORDER
	,'1020100691' -- R OB PPH RISK SUSPECTED ABRUPTION
)
and FSD.ENTRY_TIME <= #patients.OB_DELIVERY_DATE
group by #patients.PAT_ID, #patients.PAT_ENC_CSN_ID


/***************************************************************************************************
	Labs measured during pregnancy
	- Localization needed: COMPONENT_IDs, ORD_VALUEs (can also use BASE_NAMEs)
****************************************************************************************************/

drop table if exists #labs_pregnancy
select
	PAT_ID
	,PAT_ENC_CSN_ID
	,max(case when COMPONENT_ID = '13385' and ORD_VALUE = 'Positive' then 1 -- GBS DNA AMP (GBSDNAAMP)
		when COMPONENT_ID = '13385' and ORD_VALUE = 'Negative' then 0 else null end) as GBS -- GBS DNA AMP (GBSDNAAMP)
	,min(case when COMPONENT_ID = '4478' and ROW_NUM = 1 then ORD_VALUE else null end) as GLUCOSE_50G -- GLUCOSE TOL 50G (GLUCOSETOL50)
	,min(case when COMPONENT_ID = '4573' and ROW_NUM = 1 then ORD_VALUE else null end) as GLUCOSE_1H -- GLUCOSE 1 HOUR (GLUCOSE1HR)
	,min(case when COMPONENT_ID = '4587' and ROW_NUM = 1 then ORD_VALUE else null end) as GLUCOSE_2H -- GLUCOSE 2 HOUR (GLUCOSE2HR)
	,min(case when COMPONENT_ID = '4593' and ROW_NUM = 1 then ORD_VALUE else null end) as GLUCOSE_3H -- GLUCOSE 3 HOUR (GLUCOSE3HR)
into #labs_pregnancy 
from (
	select
		#patients.PAT_ID
		,#patients.PAT_ENC_CSN_ID
		,ORDER_PROC.ORDER_INST
		,ORDER_RESULTS.COMPONENT_ID
		,ORDER_RESULTS.ORD_VALUE
		,row_number() over (partition by #patients.PAT_ENC_CSN_ID, ORDER_RESULTS.COMPONENT_ID order by ORDER_PROC.ORDER_INST desc) as ROW_NUM
	from #patients
		inner join ORDER_PROC
			on ((ORDER_PROC.PAT_ID = #patients.PAT_ID)
				and (ORDER_PROC.ORDER_INST < #patients.OB_DELIVERY_DATE))
				and (datediff(month, ORDER_PROC.ORDER_INST, #patients.OB_DELIVERY_DATE) <= 9)
		inner join ORDER_RESULTS
			on (ORDER_RESULTS.ORDER_PROC_ID = ORDER_PROC.ORDER_PROC_ID)
	where ORDER_RESULTS.COMPONENT_ID in (
		'13385' -- GBS DNA AMP (GBSDNAAMP)
		,'4478' -- GLUCOSE TOL 50G (GLUCOSETOL50
		,'4573' -- GLUCOSE 1 HOUR (GLUCOSE1HR)
		,'4587' -- GLUCOSE 2 HOUR (GLUCOSE2HR)
		,'4593' -- GLUCOSE 3 HOUR (GLUCOSE3HR)
		)
) as L
group by PAT_ID
	,PAT_ENC_CSN_ID	


/***************************************************************************************************
	Labs measured prior to delivery
	- Localization needed: COMPONENT_IDs (can also use BASE_NAMEs)
****************************************************************************************************/

drop table if exists #labs
select
	PAT_ID
	,PAT_ENC_CSN_ID
	,max(case when COMPONENT_ID in (
		'93' -- HEMOGLOBIN (HGB)
		,'10624' -- HEMOGLOBIN (HEMOGLOBIN3)
		,'10477' -- HEMOGLOBIN (HGB2)
		,'4570' -- TOTAL HEMOGLOBIN WHOLE BLOOD (HGB)
		) then ORD_VALUE else NULL end) as HEMOGLOBIN
	,max(case when COMPONENT_ID in (
		'244' -- HEMATOCRIT (HCT)
		,'5510' -- HEMATOCRIT WHOLE BLOOD (HCT)
		,'10629' -- HEMATOCRIT (HEMATOCRI3)
		) then ORD_VALUE else NULL end) as HEMATOCRIT
	,max(case when COMPONENT_ID in (
		'1576930' -- MEAN CELL HEMOGLOBIN (MCH)
		,'10208' -- MEAN CORPUSCULAR HEMOGLOBIN CONCENTRATION MCHC - EXTERNAL MANUAL (POCMCHC)
		) then ORD_VALUE else NULL end) as MCH
	,max(case when COMPONENT_ID in (
		'1576932' -- MEAN CELL HEMOGLOBIN CONCENTRATION (MCHC)
		) then ORD_VALUE else NULL end) as MCHC
	,max(case when COMPONENT_ID in (
		'1576934' -- MEAN CELL VOLUME (MCV)
		) then ORD_VALUE else NULL end) as MCV
	,max(case when COMPONENT_ID in (
		'1534437' -- RDW CV (RDW)
		,'10638' -- RDW-CV (RDWCV)
		,'6807' -- POC RDW-CV (POCRDWCV)
		) then ORD_VALUE else NULL end) as RDWCV
	,max(case when COMPONENT_ID in (
		'264' -- RDW SD (RDWSD)
		,'10637' -- RDW-SD (RDWSDWMC)
		,'6806' -- POC RDW-SD (POCRDWSD)
		) then ORD_VALUE else NULL end) as RDWSD
	,max(case when COMPONENT_ID in (
		'1577876' -- WHITE BLOOD CELLS (WBC)
		,'10621' -- WBC (WBCWMC2)
		) then ORD_VALUE else NULL end) as WBC
	,max(case when COMPONENT_ID in (
		'5878' -- ABSOLUTE NEUTROPHILS (NEUTABS)
		) then ORD_VALUE else null end) as ANC
	,max(case when COMPONENT_ID in (
		'1577116' -- PLATELET (PLT)
		,'10640' -- PLATELET COUNT (PLATELETCO)
		) then ORD_VALUE else null end) as PLATELET
	,max(case when COMPONENT_ID in (
		'1537734' -- SODIUM LEVEL (NA)
		,'5295' -- SODIUM WHOLE BLOOD (NA)
		,'11239' -- SODIUM (SODIUMWMC)
		) then ORD_VALUE else null end) as NA
	,max(case when COMPONENT_ID in (
		'1534081' -- POTASSIUM LEVEL (K)
		,'5280' -- POTASSIUM WHOLE BLOOD (K)
		,'11242' -- POTASSIUM (POTASSWMC)
		,'10434' -- POTASSIUM (POTASSWSPEC)
		) then ORD_VALUE else null end) as K
	,max(case when COMPONENT_ID in (
		'1534119' -- CARBON DIOXIDE (CARDIOXIDE)
		) then ORD_VALUE else null end) as BICARB 
	,max(case when COMPONENT_ID in (
		'1534101' -- CHLORIDE LEVEL (CL)
		,'6176' -- CHLORIDE WHOLE BLOOD (CL)
		,'11246' -- CHLORIDE (CHLORI)
		) then ORD_VALUE else null end) as CL
	,max(case when COMPONENT_ID in (
		'11250' -- BUN (BUN2)
		,'10432' -- BUN (BUN3)
		,'1526068' -- BLOOD UREA NITROGEN (BUN)
		) then ORD_VALUE else null end) as BUN 
	,max(case when COMPONENT_ID in (
		'269' -- CREATININE LEVEL (CREATBLD)
		,'11255' -- CREATININE (CREATBLD)
		) then ORD_VALUE else null end) as CREAT
	,max(case when COMPONENT_ID in (
		'1526000' -- ASPARTATE AMINOTRANSFERASE (AST)
		,'11284' -- SGOT/AST (SGOTAST)
		) then ORD_VALUE else null end) as AST
	,max(case when COMPONENT_ID in (
		'1525870' -- ALANINE AMINOTRANSFERASE (ALT)
		,'11282' -- SGPT/ALT (SGPTALT)
		) then ORD_VALUE else null end) as ALT
	,max(case when COMPONENT_ID in (
		'1525848' -- ALKALINE PHOSPHATASE (ALKPHOS)
		,'11280' -- ALK PHOSPHATASE (ALKPHOSPHATA)
		) then ORD_VALUE else null end) as AP
	,max(case when COMPONENT_ID in (
		'1534076' -- BILIRUBIN TOTAL (BILITOTAL)
		,'11273' -- BILIRUBIN  TOTAL (BILITOT2)
		,'10826' -- BILIRUBIN (BILIRUBIN)
		,'11274' -- BILIRUBIN  TOTAL (BILIRUBI)
		,'10827' -- BILIRUBIN (BILIRUBIN2)
		,'6758' -- POC TOTAL BILIRUBIN (POCTOTALBILI)
		) then ORD_VALUE else null end) as BILI 
	,max(case when COMPONENT_ID in (
		'1558170' -- CALCIUM LEVEL TOTAL (CALCIUM)
		,'11260' -- CALCIUM (CALCIUM2)
		) then ORD_VALUE else null end) as CA
	,max(case when COMPONENT_ID in (
		'5304'  -- CALCIUM IONIZED WHOLE BLOOD (CAION)
		)then ORD_VALUE else null end) as ICA
	,max(case when COMPONENT_ID in (
		'11286' -- LDH (LDH2)
		,'10443' -- LDH (LDH)
		) then ORD_VALUE else null end) as LDH
	,max(case when COMPONENT_ID in (
		'4649' -- URIC ACID (URICACID)
		,'11258' -- URIC ACID (URICACI2)
		) then ORD_VALUE else null end) as URIC_ACID
	,max(case when COMPONENT_ID in (
		'5470' -- URINE 24 HOUR PROTEIN (URPROTEIN)
		,'10200' -- UR PROTEIN  24 HOUR DIFFERENT METHODOLOGY (URPROTEIN)
		) then ORD_VALUE else null end) as UP24H
	,max(case when COMPONENT_ID in (
		'1558034' -- GLUCOSE LEVEL (GLUCOSE)
		,'885' -- GLUCOSE POC (GLUB) (POCGLUCOSE)
		,'5268' -- GLUCOSE WHOLE BLOOD (GLUCOSE)
		,'11229' -- GLUCOSE (GLUCOSE7)
		) 
		and datediff(hour, ORDER_INST, OB_DELIVERY_DATE) <= 24 then ORD_VALUE else null end) as GLUCOSE -- only include 24 hours prior to delivery
into #labs
from (
	select
		#patients.PAT_ID
		,#patients.PAT_ENC_CSN_ID
		,#patients.OB_DELIVERY_DATE
		,ORDER_PROC.ORDER_INST
		,ORDER_RESULTS.COMPONENT_ID
		,ORDER_RESULTS.ORD_VALUE
		,row_number() over (partition by #patients.PAT_ENC_CSN_ID, ORDER_RESULTS.COMPONENT_ID order by ORDER_PROC.ORDER_INST desc) as ROW_NUM
	from #patients
		inner join ORDER_PROC
			on ((ORDER_PROC.PAT_ID = #patients.PAT_ID)
			and (ORDER_PROC.ORDER_INST <= #patients.OB_DELIVERY_DATE))
			and (datediff(month, ORDER_PROC.ORDER_INST, #patients.OB_DELIVERY_DATE) <= 1) -- within 1 month prior to delivery
		inner join ORDER_RESULTS
			on (ORDER_RESULTS.ORDER_PROC_ID = ORDER_PROC.ORDER_PROC_ID)
) as l where ROW_NUM = 1
group by PAT_ID
	,PAT_ENC_CSN_ID


/***************************************************************************************************
	Blood type, labs measured any time prior to delivery
	- Localization needed: COMPONENT_IDs, ORD_VALUEs (can also use BASE_NAMEs)
****************************************************************************************************/

drop table if exists #blood_type
select 
	PAT_ID
	,PAT_ENC_CSN_ID
	,max(case when COMPONENT_ID in (
		'1810065' -- ABO GROUP [TYPE] IN BLOOD (ABOTYPE)
		,'13413' -- PATIENT BLOOD TYPE TS (PATIENTBLD)
		,'2512' -- NEONATAL ABO TYPE (ABOTYPE)
		) then -- blood type
		case when ORD_VALUE like 'AB%' then 'AB' when ORD_VALUE like 'A%' then 'A' when ORD_VALUE like 'B%' then 'B'
			when ORD_VALUE like 'O%' then 'O' else NULL end 
			else NULL end) as BLOOD_TYPE
	,max(case when COMPONENT_ID in (
		'13421' -- ANTIBODY SCREEN TS (ANTIBODYSCRE)
		,'2519' -- RH TYPE (TUBE) (RHTYPE)
		,'2513' -- RH TYPE AUTO (RHTYPEA)
		,'2521' -- ABSC WITH ADD (ABSCM)
		,'1511222' -- RH TYPE IN BLOOD (RHTYPE)
		,'1631699' -- AB SCREEN  ABSA (ABSCRNABSA)
		) then -- rh factor
		case when ORD_VALUE like 'NEG%' then 'Negative' when ORD_VALUE like 'POS%' then 'Positive' else NULL end
			when COMPONENT_ID = '13413' 
				then 
				case when (ORD_VALUE like '%N' or (ORD_VALUE like '%NEG%' and ORD_VALUE not like '%GEL%')) then 'Negative'
					when (ORD_VALUE like '%P' or ORD_VALUE like '%POS%') then 'Positive' 
					else NULL end
				else NULL end) as RH_FACTOR
into #blood_type
from (
	select 
		#patients.PAT_ID
		,#patients.PAT_ENC_CSN_ID
		,ORDER_RESULTS.COMPONENT_ID
		,ORDER_RESULTS.ORD_VALUE
		,ORDER_RESULTS.RESULT_DATE
	from #patients
	inner join ORDER_PROC 
		on (ORDER_PROC.PAT_ID = #patients.PAT_ID)
	inner join ORDER_RESULTS 
		on (ORDER_RESULTS.ORDER_PROC_ID = ORDER_PROC.ORDER_PROC_ID)
	where 
		COMPONENT_ID in (
			'1810065' -- ABO GROUP [TYPE] IN BLOOD (ABOTYPE)
			,'2512' -- NEONATAL ABO TYPE (ABOTYPE)
			,'2513' -- RH TYPE AUTO (RHTYPEA)
			,'2519' -- RH TYPE (TUBE) (RHTYPE)
			,'1511222' -- RH TYPE IN BLOOD (RHTYPE)
			,'1631699' -- AB SCREEN  ABSA (ABSCRNABSA)
			,'2521' -- ABSC WITH ADD (ABSCM)
			,'13421' -- ANTIBODY SCREEN TS (ANTIBODYSCRE)
			,'13413' -- PATIENT BLOOD TYPE TS (PATIENTBLD)
			) -- blood type, rh factor
) as l
group by PAT_ID
	,PAT_ENC_CSN_ID


/***************************************************************************************************
	Postpartum labs
	- Localization needed: COMPONENT_IDs (can also use BASE_NAMEs)
****************************************************************************************************/

drop table if exists #labs_postpartum
select
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,ORDER_RESULTS.RESULT_DATE
	,min(case when COMPONENT_ID in (
		'93' -- HEMOGLOBIN (HGB)
		,'10624' -- HEMOGLOBIN (HEMOGLOBIN3)
		,'10477' -- HEMOGLOBIN (HGB2)
		,'4570' -- TOTAL HEMOGLOBIN WHOLE BLOOD (HGB)
		) then ORD_VALUE else NULL end) as HEMOGLOBIN
	,min(case when COMPONENT_ID in (
		'244' -- HEMATOCRIT (HCT)
		,'5510' -- HEMATOCRIT WHOLE BLOOD (HCT)
		,'10629' -- HEMATOCRIT (HEMATOCRI3)
		) then ORD_VALUE else NULL end) as HEMATOCRIT
into #labs_postpartum
from #patients
	inner join ORDER_PROC
		on (ORDER_PROC.PAT_ID = #patients.PAT_ID)
	inner join ORDER_RESULTS
		on ((ORDER_RESULTS.ORDER_PROC_ID = ORDER_PROC.ORDER_PROC_ID)
			and (ORDER_RESULTS.RESULT_DATE > #patients.OB_DELIVERY_DATE)
			and (datediff(hour, #patients.OB_DELIVERY_DATE, ORDER_RESULTS.RESULT_DATE) >= 4)
			and (datediff(hour, #patients.OB_DELIVERY_DATE, ORDER_RESULTS.RESULT_DATE) <= 48))
group by #patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,ORDER_RESULTS.RESULT_DATE
	

/***************************************************************************************************
	Blood transfusions
	- Localization needed: PROC_IDs
****************************************************************************************************/

drop table if exists #transfusions
select 
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,count(distinct case when ORDER_PROC.PROC_ID = '58666' -- TRANSFUSE RBC (UNITS)	(NUR619)
		then ORD_BLOOD_ADMIN.BLOOD_UNIT_NUM else null end) as RBC
	,count(distinct case when ORDER_PROC.PROC_ID = '58667' -- TRANSFUSE PLATELETS (UNITS) (NUR620)
		then ORD_BLOOD_ADMIN.BLOOD_UNIT_NUM else null end) as PLATELETS
	,count(distinct case when ORDER_PROC.PROC_ID = '58669' -- TRANSFUSE FFP (UNITS) (NUR621)
		then ORD_BLOOD_ADMIN.BLOOD_UNIT_NUM else null end) as FFP
	,count(distinct case when ORDER_PROC.PROC_ID = '58670' -- TRANSFUSE CRYOPRECIPITATE (UNITS) (NUR622)
		then ORD_BLOOD_ADMIN.BLOOD_UNIT_NUM else null end) as CRYO
into #transfusions
from #patients
	inner join ORDER_PROC
		on (ORDER_PROC.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	inner join ORDER_PROC_2
		on (ORDER_PROC_2.ORDER_PROC_ID = ORDER_PROC.ORDER_PROC_ID)
	inner join ORD_BLOOD_ADMIN
		on (ORD_BLOOD_ADMIN.ORDER_ID = ORDER_PROC.ORDER_PROC_ID)
	left join CLARITY_DEP
		on (CLARITY_DEP.DEPARTMENT_ID = ORDER_PROC_2.PAT_LOC_ID)
	left join CLARITY_SER
		on (CLARITY_SER.PROV_ID = ORDER_PROC.AUTHRZING_PROV_ID)
	left join ORDER_INSTANTIATED -- get child orders for transfusion times
		on (ORDER_INSTANTIATED.ORDER_ID = ORDER_PROC.ORDER_PROC_ID)
	left join ORDER_INSTANTIATED as PARENT
		on (PARENT.INSTNTD_ORDER_ID = ORDER_PROC.ORDER_PROC_ID)
where ORDER_PROC.ORDER_STATUS_C != '4' -- exclude canceled orders
	and ORD_BLOOD_ADMIN.BLOOD_START_INSTANT >= #patients.OB_DELIVERY_DATE
	and ORDER_PROC.FUTURE_OR_STAND is null -- ignore future/standing orders
group by #patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID

	
/***************************************************************************************************
	Medications
	- Localization needed: SIMPLE_GENERIC_C, MAR_ACTION_C, BASE_GROUPER_ID
****************************************************************************************************/

drop table if exists #medications
select
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,max(case when ZC_SIMPLE_GENERIC.NAME like 'insulin%' then 1 else 0 end) as INSULIN
	,max(case when ZC_SIMPLE_GENERIC.NAME like 'oxytocin%' then 1 else 0 end) as OXYTOCIN
	,max(case when CLARITY_MEDICATION.MEDICATION_ID = '126576' -- MAGNESIUM SULFATE 20 GRAM/500 ML (4 %) IN WATER INTRAVENOUS SOLUTION
		then 1 else 0 end) as MAGNESIUM
	,max(case when ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '1692' -- terbutaline sulfate
		then 1 else 0 end) as TERBUTALINE
	,max(case when ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C in (
		'163' -- nifedipine
		,'11701' -- nifedipine micronized
		) then 1 else 0 end) as NIFEDIPINE
	,max(case when ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C in (
		'10591' -- indomethacin, submicronized
		,'2807' -- indomethacin sodium
		,'2808' -- indomethacin sodium
		) then 1 else 0 end) as INDOMETHACIN
	,max(case when ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '2336' -- betamethasone acetate,sod phos
		then 1 else 0 end) as STEROID 
	,max(case when BASE_GROUPER_ID in (
		'4080000030' -- ERX GENERAL ALL SYSTEMIC ANTIBIOTICS
		,'4080000016' -- ERX GENERAL ANTIBIOTIC MONITORING VANCOMYCIN IV
		) -- all systemic antibiotics, general antibiotic monitoring vancomycin iv
		and ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C not in (
			'3015' -- penicillin G benzathine
			,'9766' -- penicillin G pot in 0.9% NaCl
			,'3011' -- penicillin G potassium
			,'3014' -- penicillin G procaine
			,'3010' -- penicillin G sodium
			) -- exclude penicillin G
		 then 1 else 0 end) as ANTIBIOTIC 
	,max(case when BASE_GROUPER_ID = '4080100032' -- ERX GENERAL ANTICOAGULANTS FOR PATIENT HEADER
		then 1 else 0 end) as ANTICOAGULANT
	,max(case when ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '957' -- misoprostol
		then 1 else 0 end) as CYTOTEC
into #medications
from #patients
	inner join ORDER_MED
		on (ORDER_MED.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	inner join CLARITY_MEDICATION
		on (CLARITY_MEDICATION.MEDICATION_ID = ORDER_MED.MEDICATION_ID)
	inner join ZC_SIMPLE_GENERIC
		on (ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = CLARITY_MEDICATION.SIMPLE_GENERIC_C)
	inner join MAR_ADMIN_INFO
		on (MAR_ADMIN_INFO.ORDER_MED_ID = ORDER_MED.ORDER_MED_ID)
	left join GROUPER_COMPILED_REC_LIST
		on (GROUPER_COMPILED_REC_LIST.GROUPER_RECORDS_NUMERIC_ID = CLARITY_MEDICATION.MEDICATION_ID)
where MAR_ADMIN_INFO.MAR_ACTION_C in ('1', '6', '12', '13', '124') -- Given, New Bag, Bolus, Push, Self administered
	and MAR_ADMIN_INFO.TAKEN_TIME <= #patients.OB_DELIVERY_DATE -- before the delivery
group by #patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID

	
/***************************************************************************************************
	Medications with doses
	- Localization needed: MAR_ACTION_C, CONC_NAME_C, MEDICATION_IDs
****************************************************************************************************/

drop table if exists #medication_doses
select
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,sum(case when ZC_SIMPLE_GENERIC.NAME like 'oxytocin%' -- factor in stop times after delivery
			then 
			case when MAR_START.INFUSION_RATE is null then MAR_START.SIG -- single dose
				else MAR_START.INFUSION_RATE * 
					((datediff(minute, MAR_START.TAKEN_TIME, 
						case when MAR_STOP.TAKEN_TIME is not null and MAR_STOP.TAKEN_TIME < #patients.OB_DELIVERY_DATE then MAR_STOP.TAKEN_TIME
							when ORDER_MED.ORDER_END_TIME < #patients.OB_DELIVERY_DATE and ORDER_MED.ORDER_END_TIME > MAR_START.TAKEN_TIME then ORDER_MED.ORDER_END_TIME
							else #patients.OB_DELIVERY_DATE
						end) / 60.0)
						* case when ORDER_MEDINFO.CONC_NAME_C = '37' then 100.0 -- 100 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '81' then 2.0 -- 2 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '83' then 0.06 -- 0.06 units/mL
							else 1 end)
				end
		else null end
	) as OXYTOCIN_TOTAL
	,sum(case when CLARITY_MEDICATION.MEDICATION_ID = '126576' -- MAGNESIUM SULFATE 20 GRAM/500 ML (4 %) IN WATER INTRAVENOUS SOLUTION
		then
		case when MAR_START.INFUSION_RATE is null then MAR_START.SIG -- single dose
				else MAR_START.INFUSION_RATE * 
					((datediff(minute, MAR_START.TAKEN_TIME, 
						case when MAR_STOP.TAKEN_TIME is not null and MAR_STOP.TAKEN_TIME < #patients.OB_DELIVERY_DATE then MAR_STOP.TAKEN_TIME
							when ORDER_MED.ORDER_END_TIME < #patients.OB_DELIVERY_DATE and ORDER_MED.ORDER_END_TIME > MAR_START.TAKEN_TIME then ORDER_MED.ORDER_END_TIME
							else #patients.OB_DELIVERY_DATE
						end) / 60.0)
						* case when ORDER_MEDINFO.CONC_NAME_C = '37' then 100.0 -- 100 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '81' then 2.0 -- 2 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '83' then 0.06 -- 0.06 units/mL
							else 1 end)
				end
		else null end
	) as MAGNESIUM_TOTAL
into #medication_doses
from #patients
	inner join ORDER_MED
		on (ORDER_MED.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	inner join CLARITY_MEDICATION
		on (CLARITY_MEDICATION.MEDICATION_ID = ORDER_MED.MEDICATION_ID)
	inner join ZC_SIMPLE_GENERIC
		on (ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = CLARITY_MEDICATION.SIMPLE_GENERIC_C)
	inner join ORDER_MEDINFO
		on (ORDER_MEDINFO.ORDER_MED_ID = ORDER_MED.ORDER_MED_ID)
	inner join (
		select
			ORDER_MED_ID
			,DOSE_UNIT_C
			,INFUSION_RATE
			,SIG
			,TAKEN_TIME
			,MAR_ACTION_C
			,row_number() over (partition by ORDER_MED_ID order by TAKEN_TIME) as ROW_NUM
		from MAR_ADMIN_INFO
		) as MAR_START
		on (MAR_START.ORDER_MED_ID = ORDER_MED.ORDER_MED_ID)
	left join (
		select
			ORDER_MED_ID
			,DOSE_UNIT_C
			,INFUSION_RATE
			,SIG
			,TAKEN_TIME
			,row_number() over (partition by ORDER_MED_ID order by TAKEN_TIME) as ROW_NUM
		from MAR_ADMIN_INFO
	) as MAR_STOP
		on ((MAR_STOP.ORDER_MED_ID = MAR_START.ORDER_MED_ID)
			and (MAR_STOP.ROW_NUM = MAR_START.ROW_NUM + 1)) -- get the next admin for infusions for stop time
where MAR_START.TAKEN_TIME <= #patients.OB_DELIVERY_DATE -- before the delivery
	and (ZC_SIMPLE_GENERIC.NAME like 'oxytocin%'
		or CLARITY_MEDICATION.MEDICATION_ID = '126576' -- MAGNESIUM SULFATE 20 GRAM/500 ML (4 %) IN WATER INTRAVENOUS SOLUTION
		)
	and MAR_START.MAR_ACTION_C in (
			'1' --Given
			,'114' --Started During Downtime
			,'117' --Bolus from Bag
			,'12' --Bolus
			,'124' --Self Administered Via Pump
			,'13' --Push
			,'143' --Continued Previous Bag
			,'144' --Rate/Dose Change
			,'145' --Continued by Anesthesia
			,'6' --New Bag
			,'7' --Restarted
			,'9' --Rate Change'
			)	
group by #patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID


/***************************************************************************************************
	Medications for anesthesia
	- Localization needed: MAR_ACTION_C, SIMPLE_GENERIC_C
****************************************************************************************************/

drop table if exists #medications_anesthesia
select
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,ORDER_MED.ORDER_INST
	,ORDER_MED.ORDER_MED_ID
	,CLARITY_MEDICATION.NAME
into #medications_anesthesia
from #patients
	inner join ORDER_MED
		on (ORDER_MED.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	inner join CLARITY_MEDICATION
		on (CLARITY_MEDICATION.MEDICATION_ID = ORDER_MED.MEDICATION_ID)
	inner join ZC_SIMPLE_GENERIC
		on (ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = CLARITY_MEDICATION.SIMPLE_GENERIC_C)
	inner join MAR_ADMIN_INFO
		on (MAR_ADMIN_INFO.ORDER_MED_ID = ORDER_MED.ORDER_MED_ID)
where (ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C in (
	'11025' -- succinylcholine/sod Cl,iso/PF
	,'11309' -- succinylcholine in 0.9% NaCl
	,'1587' -- succinylcholine chloride
	,'8048' -- succinylcholine in 0.9%NaCl/PF
	) -- succinylcholine
	or ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '4417' -- rocuronium bromide
	or ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '4620' -- sevoflurane
	or ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '1249') -- isoflurane
	and MAR_ADMIN_INFO.MAR_ACTION_C in ('1', '6', '12', '13', '124') -- Given, New Bag, Bolus, Push, Self administered


/***************************************************************************************************
	Medications for outcomes
	- Localization needed: MAR_ACTION_C, SIMPLE_GENERIC_C, CONC_NAME_C
****************************************************************************************************/

drop table if exists #outcome_meds
select
	#patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID
	,sum(case when ZC_SIMPLE_GENERIC.NAME like 'oxytocin%' 
			then 
			case when MAR_START.INFUSION_RATE is null then MAR_START.SIG -- single dose
				else MAR_START.INFUSION_RATE * 
					((datediff(minute, MAR_START.TAKEN_TIME, 
						case when MAR_STOP.TAKEN_TIME is not null then MAR_STOP.TAKEN_TIME
							when ORDER_MED.ORDER_END_TIME > MAR_START.TAKEN_TIME then ORDER_MED.ORDER_END_TIME
							else MAR_START.TAKEN_TIME
						end) / 60.0)
						* case when ORDER_MEDINFO.CONC_NAME_C = '37' then 100.0 -- 100 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '81' then 2.0 -- 2 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '83' then 0.06 -- 0.06 units/mL
							else 1 end)
				end
		else null end) as OXYTOCIN_TOTAL
	,sum(case when ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '1182' -- methylergonovine maleate
			then 
			case when MAR_START.INFUSION_RATE is null then MAR_START.SIG -- single dose
				else MAR_START.INFUSION_RATE * 
					((datediff(minute, MAR_START.TAKEN_TIME, 
						case when MAR_STOP.TAKEN_TIME is not null then MAR_STOP.TAKEN_TIME
							when ORDER_MED.ORDER_END_TIME > MAR_START.TAKEN_TIME then ORDER_MED.ORDER_END_TIME
							else MAR_START.TAKEN_TIME
						end) / 60.0)
						* case when ORDER_MEDINFO.CONC_NAME_C = '37' then 100.0 -- 100 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '81' then 2.0 -- 2 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '83' then 0.06 -- 0.06 units/mL
							else 1 end)
				end
		else null end) as METHERGINE_TOTAL
		,sum(case when ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '957' -- misoprostol
			then 
			case when MAR_START.INFUSION_RATE is null then MAR_START.SIG -- single dose
				else MAR_START.INFUSION_RATE * 
					((datediff(minute, MAR_START.TAKEN_TIME, 
						case when MAR_STOP.TAKEN_TIME is not null then MAR_STOP.TAKEN_TIME
							when ORDER_MED.ORDER_END_TIME > MAR_START.TAKEN_TIME then ORDER_MED.ORDER_END_TIME
							else MAR_START.TAKEN_TIME
						end) / 60.0)
						* case when ORDER_MEDINFO.CONC_NAME_C = '37' then 100.0 -- 100 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '81' then 2.0 -- 2 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '83' then 0.06 -- 0.06 units/mL
							else 1 end)
				end
		else null end) as CYTOTEC_TOTAL
		,sum(case when ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '1185' -- carboprost tromethamine
			then 
			case when MAR_START.INFUSION_RATE is null then MAR_START.SIG -- single dose
				else MAR_START.INFUSION_RATE * 
					((datediff(minute, MAR_START.TAKEN_TIME, 
						case when MAR_STOP.TAKEN_TIME is not null then MAR_STOP.TAKEN_TIME
							when ORDER_MED.ORDER_END_TIME > MAR_START.TAKEN_TIME then ORDER_MED.ORDER_END_TIME
							else MAR_START.TAKEN_TIME
						end) / 60.0)
						* case when ORDER_MEDINFO.CONC_NAME_C = '37' then 100.0 -- 100 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '81' then 2.0 -- 2 units/ml
							when ORDER_MEDINFO.CONC_NAME_C = '83' then 0.06 -- 0.06 units/mL
							else 1 end)
				end
		else null end) as HEMABATE_TOTAL
		,max(case when ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C in (
			'2263' -- tranexamic acid
			,'11854' -- tranexamic acid in NaCl,iso-os
			) then 1 else 0 end) as TXA
into #outcome_meds
from #patients
	inner join ORDER_MED
		on (ORDER_MED.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	inner join CLARITY_MEDICATION
		on (CLARITY_MEDICATION.MEDICATION_ID = ORDER_MED.MEDICATION_ID)
	inner join ZC_SIMPLE_GENERIC
		on (ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = CLARITY_MEDICATION.SIMPLE_GENERIC_C)
	inner join ORDER_MEDINFO
		on (ORDER_MEDINFO.ORDER_MED_ID = ORDER_MED.ORDER_MED_ID)
	inner join (
		select
			ORDER_MED_ID
			,DOSE_UNIT_C
			,INFUSION_RATE
			,SIG
			,TAKEN_TIME
			,MAR_ACTION_C
			,row_number() over (partition by ORDER_MED_ID order by TAKEN_TIME) as ROW_NUM
		from MAR_ADMIN_INFO
		) as MAR_START
		on (MAR_START.ORDER_MED_ID = ORDER_MED.ORDER_MED_ID)
	left join (
		select
			ORDER_MED_ID
			,DOSE_UNIT_C
			,INFUSION_RATE
			,SIG
			,TAKEN_TIME
			,row_number() over (partition by ORDER_MED_ID order by TAKEN_TIME) as ROW_NUM
		from MAR_ADMIN_INFO
	) as MAR_STOP
		on ((MAR_STOP.ORDER_MED_ID = MAR_START.ORDER_MED_ID)
			and (MAR_STOP.ROW_NUM = MAR_START.ROW_NUM + 1)) -- get the next admin for infusions for stop time
where (ZC_SIMPLE_GENERIC.NAME like 'oxytocin%'
	or ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '1182' -- methergine
	or ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '1185' -- hemabate
	or ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C = '957' -- cytotec
	or ZC_SIMPLE_GENERIC.SIMPLE_GENERIC_C in ('2263', '11854')) -- tranexamic acid
	and MAR_START.TAKEN_TIME >= #patients.OB_DELIVERY_DATE -- before the delivery
	and MAR_START.MAR_ACTION_C in (
			'1' --Given
			,'114' --Started During Downtime
			,'117' --Bolus from Bag
			,'12' --Bolus
			,'124' --Self Administered Via Pump
			,'13' --Push
			,'143' --Continued Previous Bag
			,'144' --Rate/Dose Change
			,'145' --Continued by Anesthesia
			,'6' --New Bag
			,'7' --Restarted
			,'9' --Rate Change'
			)	
group by #patients.PAT_ID
	,#patients.PAT_ENC_CSN_ID





/***************************************************************************************************
****************************************************************************************************
	Final query to pull the data together with one row per delivery
****************************************************************************************************
****************************************************************************************************/

select distinct
	#patients.PAT_MRN_ID
	,#patients.PAT_ENC_CSN_ID
	,#patients.PAT_AGE
	,#patients.OB_DELIVERY_DATE
	,#patients.HOSP_ADMSN_TIME
	,#patients.HOSP_DISCH_TIME
	,coalesce(#vitals.FIRST_HEIGHT, #preadmit_vitals.FIRST_HEIGHT) as HEIGHT
	,coalesce(#vitals.FIRST_WEIGHT, #preadmit_vitals.FIRST_WEIGHT) as ADMISSION_WEIGHT 
	,coalesce(#vitals.FIRST_BMI,#preadmit_vitals.FIRST_BMI) as ADMISSION_BMI
	,#race_ethnicity.RACE
	,#race_ethnicity.ETHNICITY
	,#patients.GEST_AGE_DAYS_TOTAL as GEST_AGE
	,#ob_hx.OB_PARITY
	,case when #ob_hx.OB_PREMATURE >= 1 then 1 else 0 end as PRIOR_PRETERM_BIRTH
	,#diagnoses.PRIOR_PPH
	,#checklist.FAMILY_HX_PPH
	,#prior_proc.PRIOR_CD_CPT as PRIOR_CD
	,case when #diagnoses.GBS = 1 then 1
		when #labs_pregnancy.GBS = 1 then 1 else 0 end as GBS
	,#patients.INSURANCE
	,#social_hx.MARITAL_STATUS
	,#social_hx.EDUCATION_LEVEL
	,#social_hx.TOBACCO_USE
	,#social_hx.DRUG_USE
	,#diagnoses.CHRONIC_HYPERTENSION
	,#diagnoses.GEST_HYPERTENSION
	,#diagnoses.PREE_NOSF
	,#diagnoses.PREE_SF
	,#diagnoses.ECLAMPSIA
	,#diagnoses.HELLP
	,#diagnoses.CHRONIC_DM
	,#diagnoses.GEST_DM
	,#diagnoses.ANEMIA
	,#diagnoses.OBESITY
	,#diagnoses.LEIOMYOMAS
	,#diagnoses.POLYHYDRAMNIOS
	,#diagnoses.MULTIPLE_GEST
	,#diagnoses.PLACENTA_PREVIA
	,#diagnoses.PLACENTAL_ABRUPTION
	,#diagnoses.PLACENTA_ACCRETA
	,#diagnoses.PLACENTA_INCRETA
	,#diagnoses.PLACENTA_PERCRETA
	,#diagnoses.ANTEPARTUM_HEMORRHAGE
	,#diagnoses.FETAL_DEMISE
	,#diagnoses.CHORIOAMNIONITIS
	,#diagnoses.ART
	,#diagnoses.SMALL_FOR_GEST_AGE
	,#diagnoses.LARGE_FOR_GEST_AGE
	,#diagnoses.SEIZURE_DISORDER
	,#diagnoses.TOBACCO_USE
	,#diagnoses.DRUG_USE
	,#diagnoses.HEART_DISEASE
	,#diagnoses.THYROID_DISEASE
	,#diagnoses.RENAL_DISEASE
	,#diagnoses.ASTHMA
	,#diagnoses.DEPRESSION
	,#diagnoses.GI_DISEASE
	,#diagnoses.THROMBOCYTOPENIA
	,#diagnoses.COAGULOPATHY
	,#diagnoses.BREECH
	,#diagnoses.PROM
	,#delivery.PLACENTA_REMOVAL 
	,case when #delivery.PRESENTATION like '%Breech%' then 1 else 0 end as BREECH
	,#vitals.FETAL_HEART_RATE
	,case when #admissions.PAT_ENC_CSN_ID is not null then 1 else 0 end as PRIOR_HOSP
	,#delivery.INDUCTION
	,#delivery.AUGMENTATION
	,case when #delivery.DELIVERY_METHOD like '%Spontaneous%' then 1 else 0 end as SPONTANEOUS_LABOR
	,#delivery.OB_DEL_1ST_STAGE_HR
	,#delivery.OB_DEL_1ST_STAGE_M 
	,#delivery.OB_DEL_2ND_STAGE_HR
	,#delivery.OB_DEL_2ND_STAGE_M 
	,case when #delivery.ANESTHESIA_TYPE like '%Epidural%' then 1 else 0 end as EPIDURAL_ANALGESIA 
	,#vitals.DTR_RUE
	,#vitals.DTR_LUE
	,#vitals.DTR_RLE
	,#vitals.RESPIRATORY_SUPPORT
	,#delivery.EPISIOTOMY
	,#delivery.LACERATIONS
	,case when #delivery.DELIVERY_METHOD like '%Forceps%' or #delivery.DELIVERY_METHOD like '%Vacuum%' then 1 else 0 end as INSTRUMENTED
	,case when #delivery.DELIVERY_METHOD like '%C-section%' then 1 else 0 end as CESAREAN_DELIVERY
	,case when #delivery.ANESTHESIA_TYPE = 'General' then 1 when #medications_anesthesia.PAT_ENC_CSN_ID is not null then 1 else 0 end as GENERAL_ANESTHESIA
	,#medications.INSULIN
	,#medications.OXYTOCIN
	,#medication_doses.OXYTOCIN_TOTAL
	,#medications.MAGNESIUM
	,#medication_doses.MAGNESIUM_TOTAL
	,#medications.TERBUTALINE
	,#medications.NIFEDIPINE
	,#medications.INDOMETHACIN
	,#medications.STEROID
	,#medications.ANTICOAGULANT
	,#medications.ANTIBIOTIC
	,#medications.CYTOTEC
	,#checklist.PRIOR_VD
	,#checklist.PRIOR_PPH
	,#checklist.PRIOR_CD
	,#checklist.FIBROIDS
	,#checklist.POLYHYDRAMNIOS
	,#checklist.PLACENTA_PREVIA
	,#checklist.PLACENTAL_ABRUPTION
	,case when #checklist.PLACENTA_ACCRETA in (1, 2) then 1 else 0 end as INVASIVE_PLACENTATION
	,#checklist.PLACENTA_ACCRETA
	,#checklist.PLACENTA_PERCRETA
	,#checklist.ACTIVE_BLEEDING
	,#checklist.FETAL_DEMISE
	,#checklist.CHORIO
	,#checklist.INDUCTION
	,#checklist.PROLONGED
	,#checklist.PROLONGED_2ND
	,#checklist.BLEEDING_DISORDER
	,#labs.HEMOGLOBIN
	,#labs.HEMATOCRIT
	,#labs.MCH
	,#labs.MCHC
	,#labs.MCV
	,#labs.RDWCV
	,#labs.RDWSD
	,#labs.WBC
	,#labs.ANC
	,#labs.PLATELET
	,#labs.NA
	,#labs.K
	,#labs.BICARB
	,#labs.CL
	,#labs.BUN
	,#labs.CREAT
	,#labs.AST
	,#labs.ALT
	,#labs.AP
	,#labs.BILI
	,#labs.CA
	,#labs.GLUCOSE as MAX_GLUCOSE
	,#labs_pregnancy.GLUCOSE_50G
	,#labs_pregnancy.GLUCOSE_1H
	,#labs_pregnancy.GLUCOSE_2H
	,#labs_pregnancy.GLUCOSE_3H
	,#blood_type.BLOOD_TYPE
	,#blood_type.RH_FACTOR
	,#vitals.FIRST_SBP
	,#vitals.MAX_SBP
	,#vitals.MIN_SBP
	,#vitals.FIRST_DBP
	,#vitals.MAX_DBP
	,#vitals.IN_DBP
	,#vitals.FIRST_PULSE
	,#vitals.MAX_PULSE
	,#vitals.MIN_PULSE
	,#vitals.FIRST_HR
	,#vitals.MAX_HR
	,#vitals.MIN_HR
	,#vitals.FIRST_RR
	,#vitals.MAX_RR
	,#vitals.MIN_RR
	,#vitals.FIRST_O2SAT
	,#vitals.MAX_O2SAT
	,#vitals.MIN_O2SAT
	,#vitals.FIRST_TEMP
	,#vitals.MAX_TEMP
	,#vitals.MIN_TEMP
	,#patients.OB_DEL_BLOOD_LOSS
	,#blood_loss.IU_TAMPONADE_OUTPUT
	,#blood_loss.VTHH_TBL
	,#blood_loss.BC_TBL
	,#blood_loss.EBL
	,#blood_loss.ONCBCN_TP_VOL_RM
	,#blood_loss.TBL
	,#labs_postpartum.HEMOGLOBIN as PP_HEMOBLOGIN
	,#labs_postpartum.HEMATOCRIT as PP_HEMATOCRIT
	,coalesce(#outcome_meds.METHERGINE_TOTAL, 0) as METHERGINE_TOTAL_POSTPARTUM
	,coalesce(#outcome_meds.HEMABATE_TOTAL, 0) as HEMABATE_TOTAL_POSTPARTUM
	,coalesce(#outcome_meds.CYTOTEC_TOTAL, 0) as CYTOTEC_TOTAL_POSTPARTUM
	,coalesce(#outcome_meds.OXYTOCIN_TOTAL, 0) as OXYTOCIN_TOTAL_POSTPARTUM
	,coalesce(#outcome_meds.TXA, 0) as TXA_POSTPARTUM
	,#transfusions.RBC
	,#transfusions.FFP
	,#transfusions.CRYO
	,#transfusions.PLATELETS
from #patients
	left join #race_ethnicity
		on (#race_ethnicity.PAT_ID = #patients.PAT_ID)
	left join #ob_hx
		on (#ob_hx.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #social_hx
		on (#social_hx.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #vitals
		on (#vitals.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #preadmit_vitals
		on (#preadmit_vitals.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #diagnoses
		on (#diagnoses.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #admissions
		on (#admissions.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #prior_proc
		on (#prior_proc.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #labs_pregnancy
		on (#labs_pregnancy.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #delivery
		on (#delivery.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #medications_anesthesia
		on (#medications_anesthesia.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #medications
		on (#medications.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #checklist
		on (#checklist.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #labs
		on (#labs.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #blood_type
		on (#blood_type.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #blood_loss
		on (#blood_loss.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #labs_postpartum
		on (#labs_postpartum.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #transfusions
		on (#transfusions.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #medication_doses
		on (#medication_doses.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)
	left join #outcome_meds
		on (#outcome_meds.PAT_ENC_CSN_ID = #patients.PAT_ENC_CSN_ID)