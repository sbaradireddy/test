WITH SQ_S10_NJ_DMV_EXTRACT AS (
SELECT
	ROW_NUMBER() OVER (
	ORDER BY TPDH.POL_PK) AS source_record_id,
	-- TPDH refers to T_POL_DIM_HST
	TPAH.STATE,
	TPAH.CNTY,
	TPAH.PSTL_CD,
	TPDH.PRE_CONV_CO_CD,
	TPAH.ADDR_PK,
	TPAH.PMRY_ADDR_FLG,
	TPAH.HSE_NUM,
	TPDH.POL_PK,
	-- Table alias for POL_PK in T_POL_DIM_HST
	TPDH.POL_NUM,
	TPDH.POL_SEQ_NUM,
	TPDH.LAPSE_BEGIN_DT,
	TPDH.LAPSE_END_DT,
	TPDH.DATA_DT,
	TPDH.PROD_CD,
	TPDH.PLN_CD,
	TPDH.CNCL_TYP,
	TPDH.POL_VRSN_TXN_TYP,
	TPDH.ROW_STAT,
	TPDH.CUR_TERM_EFF_DT,
	TPDH.CUR_TERM_XPTN_DT,
	TPDH.CNCL_RSN,
	TPDH.RCD_ACTN_TYP,
	TPFDH.PRIME_CONV_FLG,
	TPFDH.PRIME_POL_FLG,
	TPCTH.VEH_CT,
	TPDH.PRCSG_GRP_CD,
	TPDH.POL_STATE_STAT,
	TPDH.SRC_POL_ID,
	TPDH.CO_ID,
	TPDH.PROD_ID,
	TPDH.POL_STATE_VRSN,
	TPDH.POL_STATE_EFF_DT,
	TPDH.ROW_XPTN_DT,
	TPDH.CORP_CD,
	TPDH.CO_CD,
	TPDH.SRC_PROD_ID,
	TPDH.SRC_CD,
	TPDH.CNCL_EFF_DT,
	TPDH.REINST_EFF_DT,
	TPDH.ACCTG_DT,
	TPDH.REINST_TYP,
	TPPDH.DRVR_LIC_NUM,
	TPPDH.DRVR_LIC_STATE,
	TPPDH.INSD_PTY_PK,
	TPPDH.SRC_PTY_ID,
	TPPDH.NAM_FST,
	TPPDH.NAM_LST,
	TPPDH.NAM_MDL,
	TPPDH.NAM_SFX,
	TPPDH.BUS_NAM,
	TPPDH.PMRY_NAMD_INSD_FLG,
	TPPDH.NAMD_INSD_FLG,
	TPPDH.DRVR_FLG,
	TPVUDH.STAT_MAKE_CD,
	TPVUDH.MODEL_YR,
	TPVUDH.MK,
	TPVUDH.VEH_UNIT_PK,
	TPVUDH.RCD_ACTN_TYP AS RCD_ACTN_TYP_veh,
	TPVUDH.ORGL_EFF_DT,
	TPVUDH.VEH_TYP_CD,
	TPVUDH.VIN,
	TCD.SRC_CORP_ID,
	TCD.SRC_CO_ID,
	TPAH.ADDRESS1,
	TPAH.ADDRESS2,
	TPAH.ADDRESS3,
	TPAH.ADDRESS4,
	TPAH.CITY
FROM
	DWADMN.T_POL_ADDR_DIM_HST TPAH,
	-- Alias TPAH for T_POL_ADDR_DIM_HST
	DWADMN.T_POL_PTY_DIM_HST TPPDH,
	-- Alias TPPDH for T_POL_PTY_DIM_HST
	DWADMN.T_POL_FLG_DIM_HST TPFDH,
	-- Alias TPFDH for T_POL_FLG_DIM_HST
	DWADMN.T_POL_CT_DIM_HST TPCTH,
	-- Alias TPCTH for T_POL_CT_DIM_HST
	DWADMN.T_POL_VEH_UNIT_DIM_HST TPVUDH,
	-- Alias TPVUDH for T_POL_VEH_UNIT_DIM_HST
	DWADMN.T_POL_DIM_HST TPDH,
	-- Alias TPDH for T_POL_DIM_HST
	DWADMN.T_CO_DIM TCD
	-- Alias TCD for T_CO_DIM
WHERE
	TPAH.POL_PK = TPDH.POL_PK
	-- Use table alias TPAH for T_POL_ADDR_DIM_HST
	AND TPPDH.POL_PK = TPDH.POL_PK
	-- Use table alias TPPDH for T_POL_PTY_DIM_HST
	AND TPFDH.POL_PK = TPDH.POL_PK
	-- Use table alias TPFDH for T_POL_FLG_DIM_HST
	AND TPCTH.POL_PK = TPDH.POL_PK
	-- Use table alias TPCTH for T_POL_CT_DIM_HST
	AND TPVUDH.POL_PK = TPDH.POL_PK
	-- Use table alias TPVUDH for T_POL_VEH_UNIT_DIM_HST
	AND TPDH.CO_ID = TCD.CO_ID
	-- Use table alias TPDH for T_POL_DIM_HST
	AND RTRIM(TPDH.PRCSG_GRP_CD) IN ('HP', 'PL')
	AND RTRIM(TPDH.STATE_CD) = 'NJ'
	AND RTRIM(TPDH.CO_CD) IN ('HPPREF', 'HPPROP', 'HPPROP2', 'HPSIC', 'PAIPTWIN', 'PIC', 'PSIA', 'TCTIPU', 'TCTIPU2', 'ALN_HPCIC', 'ALN_TEACH', 'ALN_PSIA', 'ALN_PIC')
	AND RTRIM(TPDH.CORP_CD) IN ('HIGHPOINT', 'Palisades')
	AND (
            (RTRIM(TPDH.PROD_CD) IN ('PA', 'PAIP'))
		OR (RTRIM(TPDH.PROD_CD) = 'PIC_NEW_CA'
			AND TPDH.LEG_ENT_CD = 'A')
		OR (RTRIM(TPDH.PROD_CD) = 'PIC_NEW_CA'
			AND LTRIM(RTRIM(TPVUDH.VEH_TYP_CD)) = 'LMO'
				AND TPCTH.VEH_CT <= 5)
        )
	AND TPDH.RCD_ACTN_TYP <> 'D'
	AND (
            (TPDH.ACCTG_DT <= '2024-09-30'
		AND TPDH.POL_STATE_EFF_DT <= '2024-08-31'
		AND TPDH.POL_STATE_EFF_DT >= '2024-08-01')
	OR (TPDH.ACCTG_DT <= '2024-09-30'
		AND TPDH.ACCTG_DT >= '2024-09-01'
		AND TPDH.POL_STATE_EFF_DT < '2024-08-01')
        )
	AND ((TPDH.Pol_Vrsn_Txn_Typ = 'NB'
		AND (TPDH.conv_pol = 'N'
			OR TPDH.conv_pol IS NULL))
	OR (TPFDH.PRIME_CONV_FLG = 'Y'
		AND TPDH.CO_CD = 'ALN_PIC'
		AND TPDH.POL_VRSN_TXN_TYP = 'RN')
	OR (TPFDH.PRIME_CONV_FLG = 'Y'
		AND TPDH.CO_CD = 'ALN_PSIA'
		AND TPDH.POL_VRSN_TXN_TYP = 'RN'
		AND TPDH. PRE_CONV_CO_CD = 'PIC')
	OR (TPDH.Pol_Vrsn_Txn_Typ = 'RN'
		AND TPDH.ict_dt = TPDH.cur_term_eff_dt
		AND TPCTH.ict_ct = 1)
	OR (SUBSTR(TPDH.POL_VRSN_TXN_TYP, 1, 2) = 'EN'
		AND TPVUDH.RCD_ACTN_TYP = 'A'
		AND TPDH.POL_STATE_EFF_DT >= TPDH.CUR_TERM_EFF_DT)
	OR (RTRIM(LTRIM(TPDH.POL_VRSN_TXN_TYP)) IN ('CN', 'CNX')
		AND RTRIM(LTRIM(TPDH.CNCL_RSN)) IN ('NONPAY', 'NP', 'NP_CR', 'NP_PF'))
		OR (SUBSTR(TPDH.POL_VRSN_TXN_TYP, 1, 2) = 'RS'
			AND RTRIM(LTRIM(TPDH.REINST_TYP)) = 'WITH LAPSE'
				AND RTRIM(LTRIM(TPDH.CNCL_RSN)) IN ('NONPAY', 'NP', 'NP_CR', 'NP_PF'))
			OR (SUBSTR(TPDH.POL_VRSN_TXN_TYP, 1, 2) = 'RS'
				AND RTRIM(LTRIM(TPDH.REINST_TYP)) = 'REINSTATE'
					AND RTRIM(LTRIM(TPDH.CNCL_RSN)) IN ('NONPAY', 'NP', 'NP_CR', 'NP_PF')
						AND TPDH.ROW_STAT = 'C'))
	AND TPVUDH.Veh_Typ_Cd IN ('AQ', 'CL', 'CV', 'PP', 'PPA', 'PPP', 'LMO', 'VAN')
	AND TPPDH.Pmry_Namd_Insd_Flg = 'Y'
	AND TPAH.Pmry_Addr_Flg = 'Y'
	AND (TPFDH.MOTORCYCLE_POL_FLG != 'Y'
		OR TPFDH.MOTORCYCLE_POL_FLG IS NULL)
	AND NOT EXISTS (
	SELECT
		*
	FROM
		ERM.T_ESS_SMOKETEST_POLICIES
	WHERE
		ERM.T_ESS_SMOKETEST_POLICIES.STATE_CD = TPDH.STATE_CD
		AND ERM.T_ESS_SMOKETEST_POLICIES.POL_NUM = TPDH.POL_NUM
               )
       )
--  select * from SQ_S10_NJ_DMV_EXTRACT
,
EXPTRANS AS (
SELECT
	SQ_S10_NJ_DMV_EXTRACT.source_record_id,
	SQ_S10_NJ_DMV_EXTRACT.POL_PK,
	SQ_S10_NJ_DMV_EXTRACT.POL_NUM,
	SQ_S10_NJ_DMV_EXTRACT.POL_SEQ_NUM,
	SQ_S10_NJ_DMV_EXTRACT.POL_STATE_VRSN,
	SQ_S10_NJ_DMV_EXTRACT.POL_STATE_EFF_DT,
	SQ_S10_NJ_DMV_EXTRACT.ROW_XPTN_DT,
	SQ_S10_NJ_DMV_EXTRACT.POL_VRSN_TXN_TYP,
	SQ_S10_NJ_DMV_EXTRACT.ROW_STAT,
	SQ_S10_NJ_DMV_EXTRACT.RCD_ACTN_TYP,
	SQ_S10_NJ_DMV_EXTRACT.DATA_DT,
	SQ_S10_NJ_DMV_EXTRACT.PRIME_POL_FLG,
	SQ_S10_NJ_DMV_EXTRACT.PRCSG_GRP_CD,
	SQ_S10_NJ_DMV_EXTRACT.POL_STATE_STAT,
	SQ_S10_NJ_DMV_EXTRACT.SRC_POL_ID,
	SQ_S10_NJ_DMV_EXTRACT.CO_ID,
	SQ_S10_NJ_DMV_EXTRACT.PROD_ID,
	SQ_S10_NJ_DMV_EXTRACT.CORP_CD,
	SQ_S10_NJ_DMV_EXTRACT.CO_CD,
	SQ_S10_NJ_DMV_EXTRACT.SRC_PROD_ID,
	SQ_S10_NJ_DMV_EXTRACT.PROD_CD,
	SQ_S10_NJ_DMV_EXTRACT.PLN_CD,
	SQ_S10_NJ_DMV_EXTRACT.CUR_TERM_EFF_DT,
	SQ_S10_NJ_DMV_EXTRACT.CUR_TERM_XPTN_DT,
	SQ_S10_NJ_DMV_EXTRACT.CNCL_RSN,
	SQ_S10_NJ_DMV_EXTRACT.CNCL_TYP,
	SQ_S10_NJ_DMV_EXTRACT.CNCL_EFF_DT,
	SQ_S10_NJ_DMV_EXTRACT.REINST_EFF_DT,
	SQ_S10_NJ_DMV_EXTRACT.ACCTG_DT,
	SQ_S10_NJ_DMV_EXTRACT.REINST_TYP,
	SQ_S10_NJ_DMV_EXTRACT.LAPSE_BEGIN_DT,
	SQ_S10_NJ_DMV_EXTRACT.LAPSE_END_DT,
	SQ_S10_NJ_DMV_EXTRACT.VEH_UNIT_PK,
	SQ_S10_NJ_DMV_EXTRACT.RCD_ACTN_TYP_veh,
	SQ_S10_NJ_DMV_EXTRACT.ORGL_EFF_DT,
	SQ_S10_NJ_DMV_EXTRACT.VEH_TYP_CD,
	SQ_S10_NJ_DMV_EXTRACT.VIN,
	SQ_S10_NJ_DMV_EXTRACT.MODEL_YR,
	SQ_S10_NJ_DMV_EXTRACT.MK,
	SQ_S10_NJ_DMV_EXTRACT.STAT_MAKE_CD,
	SQ_S10_NJ_DMV_EXTRACT.ADDR_PK,
	SQ_S10_NJ_DMV_EXTRACT.PMRY_ADDR_FLG,
	SQ_S10_NJ_DMV_EXTRACT.HSE_NUM,
	SQ_S10_NJ_DMV_EXTRACT.ADDRESS1,
	SQ_S10_NJ_DMV_EXTRACT.ADDRESS2,
	SQ_S10_NJ_DMV_EXTRACT.ADDRESS3,
	SQ_S10_NJ_DMV_EXTRACT.ADDRESS4,
	SQ_S10_NJ_DMV_EXTRACT.CITY,
	SQ_S10_NJ_DMV_EXTRACT.STATE,
	SQ_S10_NJ_DMV_EXTRACT.CNTY,
	SQ_S10_NJ_DMV_EXTRACT.PSTL_CD,
	SQ_S10_NJ_DMV_EXTRACT.INSD_PTY_PK,
	SQ_S10_NJ_DMV_EXTRACT.SRC_PTY_ID,
	SQ_S10_NJ_DMV_EXTRACT.NAM_FST,
	SQ_S10_NJ_DMV_EXTRACT.NAM_LST,
	SQ_S10_NJ_DMV_EXTRACT.NAM_MDL,
	SQ_S10_NJ_DMV_EXTRACT.NAM_SFX,
	SQ_S10_NJ_DMV_EXTRACT.BUS_NAM,
	SQ_S10_NJ_DMV_EXTRACT.PMRY_NAMD_INSD_FLG,
	SQ_S10_NJ_DMV_EXTRACT.NAMD_INSD_FLG,
	SQ_S10_NJ_DMV_EXTRACT.DRVR_FLG,
	SQ_S10_NJ_DMV_EXTRACT.DRVR_LIC_NUM,
	SQ_S10_NJ_DMV_EXTRACT.DRVR_LIC_STATE,
	-- Transformation Columns
	SUBSTR(SQ_S10_NJ_DMV_EXTRACT.PSTL_CD, 1, 5) AS ZIP_FIRST5_CD,
	SUBSTR(SQ_S10_NJ_DMV_EXTRACT.PSTL_CD, 7, 4) AS ZIP_PLUS_CD,
	REGEXP_REPLACE('2024-09-01', CHR(39), NULL, 1, 0, 'c') AS v_BATCH_START_DT,
	REGEXP_REPLACE('2024-09-30', CHR(39), NULL, 1, 0, 'c') AS v_BATCH_END_DT,
	REGEXP_REPLACE('2024-08-01', CHR(39), NULL, 1, 0, 'c') AS v_RPRTG_PERIOD_START_DT,
	REGEXP_REPLACE('2024-08-31', CHR(39), NULL, 1, 0, 'c') AS v_RPRTG_PERIOD_END_DT,
	IFF(
        SUBSTR(SQ_S10_NJ_DMV_EXTRACT.POL_VRSN_TXN_TYP, 1, 2) = 'RN'
		AND SQ_S10_NJ_DMV_EXTRACT.PRIME_CONV_FLG = 'Y'
		AND SQ_S10_NJ_DMV_EXTRACT.CO_CD = 'ALN_PSIA'
		AND SQ_S10_NJ_DMV_EXTRACT.PRE_CONV_CO_CD = 'PIC',
		'NB',
		IFF(
            SUBSTR(SQ_S10_NJ_DMV_EXTRACT.POL_VRSN_TXN_TYP, 1, 2) = 'RN'
			AND SQ_S10_NJ_DMV_EXTRACT.PRIME_CONV_FLG = 'Y'
			AND SQ_S10_NJ_DMV_EXTRACT.CO_CD = 'ALN_PIC',
			'NB',
			IFF(
                SUBSTR(SQ_S10_NJ_DMV_EXTRACT.POL_VRSN_TXN_TYP, 1, 2) = 'NB',
			'NB',
			IFF(
                    SUBSTR(SQ_S10_NJ_DMV_EXTRACT.POL_VRSN_TXN_TYP, 1, 2) = 'CN',
			'CN',
			IFF(
                        SUBSTR(SQ_S10_NJ_DMV_EXTRACT.POL_VRSN_TXN_TYP, 1, 2) = 'RN',
			'RN',
			IFF(
                            SUBSTR(SQ_S10_NJ_DMV_EXTRACT.POL_VRSN_TXN_TYP, 1, 2) = 'EN',
			'AV',
			IFF(
                                SUBSTR(SQ_S10_NJ_DMV_EXTRACT.POL_VRSN_TXN_TYP, 1, 2) = 'RS',
			'RS',
			''
                            )
                        )
                    )
                )
            )
        )
    ) AS DMV_TRANS_TYPE,
	UPPER(SQ_S10_NJ_DMV_EXTRACT.MK) AS MK_caps,
	RPAD(UPPER(LTRIM(RTRIM(SQ_S10_NJ_DMV_EXTRACT.VIN))), 19, ' ') AS DMV_VIN,
	IFF(
        SQ_S10_NJ_DMV_EXTRACT.DRVR_LIC_NUM IS NULL
		OR SQ_S10_NJ_DMV_EXTRACT.DRVR_LIC_STATE != 'NJ',
		'               ',
		RPAD(UPPER(LTRIM(RTRIM(SQ_S10_NJ_DMV_EXTRACT.DRVR_LIC_NUM))), 15, ' ')
    ) AS DMV_DRIVER_LICENSE_NUMBER,
	RPAD(UPPER(LTRIM(RTRIM(SQ_S10_NJ_DMV_EXTRACT.STAT_MAKE_CD))), 5, ' ') AS DMV_MAKE_OF_CAR,
	IFF(
        SQ_S10_NJ_DMV_EXTRACT.MODEL_YR IS NULL
		OR SQ_S10_NJ_DMV_EXTRACT.MODEL_YR = 0,
		'     ',
		RPAD(LTRIM(RTRIM(TO_CHAR(SQ_S10_NJ_DMV_EXTRACT.MODEL_YR))), 4, ' ')
    ) AS DMV_YEAR_OF_CAR,
	'     ' AS DMV_MODEL_OF_CAR,
	DECODE(
        RTRIM(LTRIM(SQ_S10_NJ_DMV_EXTRACT.CO_CD)),
        'PIC', '4895',
        'PSIA', '4840',
        'HPPREF', '4808',
        'HPSIC', '4875',
        'HPPROP', '4876',
        'HPPROP2', '4876',
        'TCTIPU', '4946',
        'TCTIPU2', '4946',
        'PAIPTWIN', '4927',
        'ALN_HPCIC', '4876',
        'ALN_TEACH', '4946',
        'ALN_PSIA', '4840',
        'ALN_PIC', '4895',
        'XXXX'
    ) AS DMV_INS_COMPANY_CD,
	RPAD(UPPER(LTRIM(RTRIM(SQ_S10_NJ_DMV_EXTRACT.HSE_NUM))) || ' ' || UPPER(LTRIM(RTRIM(SQ_S10_NJ_DMV_EXTRACT.ADDRESS1))), 30, ' ') AS DMV_POLICY_OWNER_STREET_ADDR,
	RPAD(UPPER(LTRIM(RTRIM(SQ_S10_NJ_DMV_EXTRACT.CITY))), 20, ' ') AS DMV_POLICY_OWNER_CITY,
	RPAD(UPPER(LTRIM(RTRIM(SQ_S10_NJ_DMV_EXTRACT.STATE))), 2, ' ') AS DMV_POLICY_OWNER_STATE,
	RPAD(LTRIM(RTRIM(ZIP_FIRST5_CD)), 9, ' ') AS DMV_POLICY_OWNER_ZIP_CODE,
	IFF(SUBSTR(SQ_S10_NJ_DMV_EXTRACT.POL_VRSN_TXN_TYP, 1, 1) = 'C',
	'C',
	'N') AS DMV_TRANSACTION_TYPE_CODE,
	IFF(SUBSTR(SQ_S10_NJ_DMV_EXTRACT.POL_VRSN_TXN_TYP, 1, 2) = 'CN',
	'00000000',
	TO_CHAR(SQ_S10_NJ_DMV_EXTRACT.POL_STATE_EFF_DT, 'MMDDYYYY')) AS DMV_POLICY_EFFECTIVE_DATE,
	IFF(SUBSTR(SQ_S10_NJ_DMV_EXTRACT.POL_VRSN_TXN_TYP, 1, 1) = 'C',
	TO_CHAR(SQ_S10_NJ_DMV_EXTRACT.POL_STATE_EFF_DT, 'MMDDYYYY'),
	'00000000') AS DMV_POLICY_CANCELLATION_DATE,
	TO_CHAR(SYSDATE(), 'MMDDYYYY') AS DMV_DATE_STAMP,
	RPAD(LTRIM(RTRIM(SQ_S10_NJ_DMV_EXTRACT.POL_NUM)), 30, ' ') AS DMV_POLICY_NUMBER,
	RPAD(' ', 31) AS DMV_RESERVED,
	v_BATCH_START_DT,
	'2024-09-01' AS BATCH_START_DT,
	v_BATCH_END_DT,
	'2024-09-30' AS BATCH_END_DT,
	v_RPRTG_PERIOD_START_DT,
	'2024-09-01' AS RPRTG_PERIOD_START_DT,
	v_RPRTG_PERIOD_END_DT,
	'2024-09-30' AS RPRTG_PERIOD_END_DT,
	LKP_ALT_LIC_NUM.INSD_PTY_PK AS lkp_INSD_PTY_PK,
	LKP_ALT_LIC_NUM.SRC_PTY_ID AS lkp_SRC_PTY_ID,
	LKP_ALT_LIC_NUM.NAM_FST AS lkp_NAM_FST,
	LKP_ALT_LIC_NUM.NAM_LST AS lkp_NAM_LST,
	LKP_ALT_LIC_NUM.NAM_MDL AS lkp_NAM_MDL,
	LKP_ALT_LIC_NUM.NAM_SFX AS lkp_NAM_SFX,
	LKP_ALT_LIC_NUM.PMRY_NAMD_INSD_FLG AS lkp_PMRY_NAMD_INSD_FLG,
	LKP_ALT_LIC_NUM.DRVR_FLG AS lkp_DRVR_FLG,
	LKP_ALT_LIC_NUM.DRVR_LIC_NUM AS lkp_DRVR_LIC_NUM,
	LKP_ALT_LIC_NUM.DRVR_LIC_STATE AS lkp_DRVR_LIC_STATE,
	LKP_NCIC_VEH_MAKE_CODE.NCIC_VAL
FROM
	SQ_S10_NJ_DMV_EXTRACT SQ_S10_NJ_DMV_EXTRACT
	--select * from exptrans
LEFT JOIN (
    SELECT 
        T_POL_PTY_DIM_HST.INSD_PTY_PK AS INSD_PTY_PK,
        T_POL_PTY_DIM_HST.SRC_PTY_ID AS SRC_PTY_ID,
        T_POL_PTY_DIM_HST.NAM_FST AS NAM_FST,
        T_POL_PTY_DIM_HST.NAM_LST AS NAM_LST,
        T_POL_PTY_DIM_HST.NAM_MDL AS NAM_MDL,
        T_POL_PTY_DIM_HST.NAM_SFX AS NAM_SFX,
        T_POL_PTY_DIM_HST.PMRY_NAMD_INSD_FLG AS PMRY_NAMD_INSD_FLG,
        T_POL_PTY_DIM_HST.NAMD_INSD_FLG AS NAMD_INSD_FLG,
        T_POL_PTY_DIM_HST.DRVR_FLG AS DRVR_FLG,
        T_POL_PTY_DIM_HST.DRVR_LIC_NUM AS DRVR_LIC_NUM,
        T_POL_PTY_DIM_HST.DRVR_LIC_STATE AS DRVR_LIC_STATE,
        T_POL_PTY_DIM_HST.POL_PK AS POL_PK,
        ROW_NUMBER() OVER (PARTITION BY POL_PK ORDER BY POL_PK) AS rn
    FROM 
        CDWQA.DWADMN.T_POL_PTY_DIM_HST
    QUALIFY rn = 1
) LKP_ALT_LIC_NUM
ON LKP_ALT_LIC_NUM.POL_PK = SQ_S10_NJ_DMV_EXTRACT.POL_PK

LEFT JOIN (
    SELECT 
        NCIC_VAL,
        VEH_MAKE_CD,
        ROW_NUMBER() OVER (PARTITION BY VEH_MAKE_CD ORDER BY VEH_MAKE_CD) AS rn
    FROM 
        rdmdev.burrpt.NCIC_CODES_XREF
    QUALIFY rn = 1
) LKP_NCIC_VEH_MAKE_CODE
ON LKP_NCIC_VEH_MAKE_CODE.VEH_MAKE_CD = SQ_S10_NJ_DMV_EXTRACT.MK

)

,


EXP_PRIMARY_DRIVER AS (
-- writting query for expression function
SELECT
	EXPTRANS.source_record_id,
	lkp_INSD_PTY_PK AS INSD_PTY_PK_PRMRY_DRVR,
	lkp_SRC_PTY_ID AS SRC_PTY_ID_PRMRY_DRVR,
	lkp_NAM_FST AS NAM_FST_PRMRY_DRVR,
	lkp_NAM_LST AS NAM_LST_PRMRY_DRVR,
	lkp_NAM_MDL AS NAM_MDL__PRMRY_DRVR,
	lkp_NAM_SFX AS NAM_SFX_PRMRY_DRVR,
	lkp_PMRY_NAMD_INSD_FLG AS NAMD_INSD_FLG_PRMRY_DRVR,
	lkp_DRVR_FLG AS DRVR_FLG_PRMRY_DRVR,
	lkp_DRVR_LIC_NUM AS DRVR_LIC_NUM_PRMRY_DRVR,
	lkp_DRVR_LIC_STATE AS DRVR_LIC_STATE__PRMRY_DRVR
FROM
	EXPTRANS
       ) 
      -- SELECT * FROM EXPTRANS
      ,
EXPTRANS1 as (
        -- writting query for expression function
SELECT EXPTRANS.source_record_id,
              -- NEXTVAL --(Use the sequence created in snowflake)
 --AS NJ_DMV_UMIS_MTH_BUILD_ID,
               EXPTRANS.POL_PK,
               EXPTRANS.POL_NUM,
               EXPTRANS.POL_SEQ_NUM,
               NULL AS ORIG_POL_PK,
               EXPTRANS.POL_STATE_VRSN,
               EXPTRANS.POL_STATE_EFF_DT,
               EXPTRANS.ACCTG_DT,
               EXPTRANS.POL_STATE_STAT,
               EXPTRANS.ROW_XPTN_DT,
               EXPTRANS.POL_VRSN_TXN_TYP,
               EXPTRANS.ROW_STAT,
               EXPTRANS.RCD_ACTN_TYP AS RCD_ACTN_TYP_POL,
               EXPTRANS.DATA_DT,
               EXPTRANS.CORP_CD,
               EXPTRANS.CO_CD,
               EXPTRANS.SRC_PROD_ID,
              -- EXPTRANS.SRC_CD,
               EXPTRANS.SRC_POL_ID,
               EXPTRANS.CO_ID,
               EXPTRANS.PROD_ID,
               EXPTRANS.CUR_TERM_EFF_DT,
               EXPTRANS.CUR_TERM_XPTN_DT,
               EXPTRANS.CNCL_RSN,
               EXPTRANS.CNCL_TYP,
               EXPTRANS.CNCL_EFF_DT,
               EXPTRANS.REINST_EFF_DT,
              -- EXPTRANS.REINST_RSN,
               EXPTRANS.LAPSE_BEGIN_DT,
               EXPTRANS.LAPSE_END_DT,
               EXPTRANS.PLN_CD,
              -- EXPTRANS.PRIME_CONV_FLG,
               EXPTRANS.DMV_TRANS_TYPE,
               EXPTRANS.VEH_UNIT_PK,
               EXPTRANS.RCD_ACTN_TYP_veh AS RCD_ACTN_TYP_VEH,
               EXPTRANS.ORGL_EFF_DT,
               EXPTRANS.VEH_TYP_CD,
               EXPTRANS.VIN,
               EXPTRANS.MODEL_YR,
               EXPTRANS.MK,
               EXPTRANS.STAT_MAKE_CD,
               EXPTRANS.NCIC_VAL AS NCIC_VEH_MAKE,
               EXPTRANS.ADDR_PK,
               EXPTRANS.PMRY_ADDR_FLG,
               EXPTRANS.HSE_NUM,
               EXPTRANS.ADDRESS1,
               EXPTRANS.ADDRESS2,
               EXPTRANS.ADDRESS3,
               EXPTRANS.ADDRESS4,
               EXPTRANS.CITY,
               EXPTRANS.STATE,
               EXPTRANS.CNTY,
               EXPTRANS.PSTL_CD,
               EXPTRANS.INSD_PTY_PK AS INSD_PTY_PK_POL_OWNER,
               EXPTRANS.SRC_PTY_ID AS SRC_PTY_ID_POL_OWNER,
               EXPTRANS.NAM_FST AS NAM_FST_POL_OWNER,
               EXPTRANS.NAM_LST AS NAM_LST_POL_OWNER,
               EXPTRANS.NAM_MDL AS NAM_MDL_POL_OWNER,
               EXPTRANS.NAM_SFX AS NAM_SFX_POL_OWNER,
               EXPTRANS.BUS_NAM AS BUS_NAM_POL_OWNER,
               EXPTRANS.PMRY_NAMD_INSD_FLG AS PMRY_NAMD_INSD_FLG_POL_OWNER,
               EXPTRANS.NAMD_INSD_FLG AS NAMD_INSD_FLG_POL_OWNER,
               EXPTRANS.DRVR_FLG AS DRVR_FLG_POL_OWNER,
               EXPTRANS.DRVR_LIC_NUM AS DRVR_LIC_NUM_POL_OWNER,
               EXPTRANS.DRVR_LIC_STATE AS DRVR_LIC_STATE_POL_OWNER,
               NULL AS ASSOCN_TYP_PRMRY_DRVR,
               EXP_PRIMARY_DRIVER.INSD_PTY_PK_PRMRY_DRVR,
               EXP_PRIMARY_DRIVER.SRC_PTY_ID_PRMRY_DRVR,
               EXP_PRIMARY_DRIVER.NAM_FST_PRMRY_DRVR,
               EXP_PRIMARY_DRIVER.NAM_LST_PRMRY_DRVR,
               EXP_PRIMARY_DRIVER.NAM_MDL__PRMRY_DRVR,
               EXP_PRIMARY_DRIVER.NAM_SFX_PRMRY_DRVR,
               EXP_PRIMARY_DRIVER.NAMD_INSD_FLG_PRMRY_DRVR,
               EXP_PRIMARY_DRIVER.DRVR_FLG_PRMRY_DRVR,
               EXP_PRIMARY_DRIVER.DRVR_LIC_NUM_PRMRY_DRVR,
               EXP_PRIMARY_DRIVER.DRVR_LIC_STATE__PRMRY_DRVR,
               NULL AS BATCH_ID,
               EXPTRANS.BATCH_START_DT,
               EXPTRANS.BATCH_END_DT,
               EXPTRANS.RPRTG_PERIOD_START_DT,
               EXPTRANS.RPRTG_PERIOD_END_DT,
               EXPTRANS.DMV_VIN,
               EXPTRANS.DMV_MAKE_OF_CAR,
               EXPTRANS.DMV_YEAR_OF_CAR,
               EXPTRANS.DMV_MODEL_OF_CAR,
               EXPTRANS.DMV_INS_COMPANY_CD,
               EXPTRANS.DMV_POLICY_OWNER_STREET_ADDR,
               EXPTRANS.DMV_POLICY_OWNER_CITY,
               EXPTRANS.DMV_POLICY_OWNER_STATE,
               EXPTRANS.DMV_POLICY_OWNER_ZIP_CODE,
               EXPTRANS.DMV_TRANSACTION_TYPE_CODE,
               EXPTRANS.DMV_POLICY_EFFECTIVE_DATE,
               EXPTRANS.DMV_POLICY_CANCELLATION_DATE,
               EXPTRANS.DMV_DATE_STAMP,
               EXPTRANS.DMV_POLICY_NUMBER,
               EXPTRANS.DMV_RESERVED,
               EXPTRANS.PRCSG_GRP_CD,
               EXPTRANS.PROD_CD,
               EXPTRANS.REINST_TYP,
              -- EXPTRANS.SRC_CORP_ID,
              -- EXPTRANS.SRC_CO_ID,
               EXPTRANS.PRIME_POL_FLG,
              -- EXPTRANS.PRE_CONV_CO_CD,
               IFF(LENGTH(DRVR_LIC_NUM_POL_OWNER) >= 15 AND UPPER(SUBSTR(DRVR_LIC_NUM_POL_OWNER,1,1)) IN( 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'), 'Y','N') AS POL_OWNER_LIC_LIKELY_NJ_IND,
               IFF(LENGTH(DRVR_LIC_NUM_PRMRY_DRVR) >= 15 AND UPPER(SUBSTR(DRVR_LIC_NUM_PRMRY_DRVR,1,1)) IN( 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'), 'Y','N') AS PRMRY_DRVR_LIC_LIKELY_NJ_IND,
               SUBSTR(PSTL_CD,1,5) AS ZIP_FIRST5_CD,
               SUBSTR(PSTL_CD,7,4) AS ZIP_LAST4_CD,
               SYSDATE() AS CREATE_DT,
               'M_S10_NJ_DMV_EXTRACT' AS CREATED_BY,
               IFF( DRVR_LIC_NUM_POL_OWNER IS NULL AND DRVR_LIC_NUM_PRMRY_DRVR IS NULL, '               ', IFF(DRVR_LIC_STATE_POL_OWNER = 'NJ' AND DRVR_LIC_NUM_PRMRY_DRVR IS NULL, RPAD(UPPER(DRVR_LIC_NUM_POL_OWNER),15,' '), IFF(DRVR_LIC_STATE__PRMRY_DRVR = 'NJ', RPAD(UPPER(DRVR_LIC_NUM_PRMRY_DRVR),15,' '), IFF(POL_OWNER_LIC_LIKELY_NJ_IND = 'Y', RPAD(UPPER(DRVR_LIC_NUM_POL_OWNER),15,' '), IFF(PRMRY_DRVR_LIC_LIKELY_NJ_IND = 'Y', RPAD(UPPER(DRVR_LIC_NUM_PRMRY_DRVR),15,' '), '               '))))) AS DMV_DRIVER_LICENSE_NUMBER,
               IFF( DMV_MAKE_OF_CAR IS NULL AND NCIC_VEH_MAKE IS NULL AND MK IS NULL, '     ', IFF( DMV_MAKE_OF_CAR IS NULL AND NCIC_VEH_MAKE IS NULL,RPAD(UPPER(SUBSTR(RTRIM(LTRIM(MK)),1,5)),5,' '), IFF( DMV_MAKE_OF_CAR IS NULL, RPAD(SUBSTR(RTRIM(LTRIM(NCIC_VEH_MAKE)),1,5),5,' '), UPPER(DMV_MAKE_OF_CAR)))) AS DMV_MAKE_OF_CAR_out
          FROM EXPTRANS
         INNER JOIN EXP_PRIMARY_DRIVER
            ON EXPTRANS.source_record_id = EXP_PRIMARY_DRIVER.source_record_id
       ) 
     --  select * from exptrans1
     ,
UPDTRANS as (
        -- writting query for update strategy function
 SELECT EXPTRANS1.source_record_id,
               ADDR_PK,
               PMRY_ADDR_FLG,
               HSE_NUM,
               ADDRESS1,
               ADDRESS2,
               ADDRESS3,
               ADDRESS4,
               BATCH_START_DT,
               RPRTG_PERIOD_END_DT,
               DMV_VIN,
               BATCH_END_DT,
               RPRTG_PERIOD_START_DT,
               DMV_POLICY_EFFECTIVE_DATE,
               CNCL_RSN,
               DRVR_LIC_STATE_POL_OWNER,
               NAM_FST_POL_OWNER,
               NAMD_INSD_FLG_POL_OWNER,
               ZIP_FIRST5_CD,
               REINST_TYP,
               CO_CD,
               SRC_PROD_ID,
              -- SRC_CD,
               SRC_POL_ID,
               PSTL_CD,
               CITY,
               STATE,
               CNTY,
               RCD_ACTN_TYP_POL,
               DATA_DT,
               CORP_CD,
               DRVR_FLG_POL_OWNER,
               DRVR_LIC_NUM_POL_OWNER,
               DMV_DATE_STAMP,
               DMV_POLICY_NUMBER,
               DMV_RESERVED,
               PRCSG_GRP_CD,
               PROD_CD,
              -- SRC_CORP_ID,
              -- SRC_CO_ID,
               DMV_POLICY_OWNER_ZIP_CODE,
               DMV_TRANSACTION_TYPE_CODE,
               DMV_POLICY_OWNER_STATE,
               CREATED_BY,
               CUR_TERM_EFF_DT,
               CUR_TERM_XPTN_DT,
               DMV_POLICY_OWNER_CITY,
               VEH_UNIT_PK,
               RCD_ACTN_TYP_VEH,
               ORGL_EFF_DT,
               REINST_EFF_DT,
              -- REINST_RSN,
               LAPSE_BEGIN_DT,
               LAPSE_END_DT,
               PLN_CD,
               DMV_TRANS_TYPE,
               INSD_PTY_PK_POL_OWNER,
               SRC_PTY_ID_POL_OWNER,
               ACCTG_DT,
               POL_STATE_STAT,
               ROW_XPTN_DT,
               POL_VRSN_TXN_TYP,
               ROW_STAT,
               CO_ID,
               PROD_ID,
               NAM_LST_POL_OWNER,
               NAM_MDL_POL_OWNER,
               NAM_SFX_POL_OWNER,
               BUS_NAM_POL_OWNER,
               PMRY_NAMD_INSD_FLG_POL_OWNER,
               DMV_MAKE_OF_CAR_out AS DMV_MAKE_OF_CAR,
             --  NJ_DMV_UMIS_MTH_BUILD_ID,
               POL_PK,
               POL_NUM,
               POL_SEQ_NUM,
               ORIG_POL_PK,
               POL_STATE_VRSN,
               POL_STATE_EFF_DT,
               DMV_POLICY_CANCELLATION_DATE,
               NAM_FST_PRMRY_DRVR,
               NAM_LST_PRMRY_DRVR,
               NAM_MDL__PRMRY_DRVR,
               NAM_SFX_PRMRY_DRVR,
               NAMD_INSD_FLG_PRMRY_DRVR,
               DRVR_FLG_PRMRY_DRVR,
               DRVR_LIC_NUM_PRMRY_DRVR,
               DRVR_LIC_STATE__PRMRY_DRVR,
               VEH_TYP_CD,
               VIN,
               MODEL_YR,
               MK,
               STAT_MAKE_CD,
               CNCL_TYP,
               ZIP_LAST4_CD,
               ASSOCN_TYP_PRMRY_DRVR,
               INSD_PTY_PK_PRMRY_DRVR,
               SRC_PTY_ID_PRMRY_DRVR,
               CNCL_EFF_DT,
               BATCH_ID,
               CREATE_DT,
               DMV_DRIVER_LICENSE_NUMBER,
               DMV_YEAR_OF_CAR,
               DMV_MODEL_OF_CAR,
               DMV_INS_COMPANY_CD,
               DMV_POLICY_OWNER_STREET_ADDR,
              -- PRE_CONV_CO_CD
          FROM EXPTRANS1
       ) 
     --  select * from updtrans
       ,

FINAL_SELECT_S10_NJ_DMV_MTHLY_EXTRACT AS(
        -- writing query for target definition
SELECT --NJ_DMV_UMIS_MTH_BUILD_ID :: INTEGER AS NJ_DMV_UMIS_MTH_BUILD_ID,
               POL_PK :: INTEGER AS POL_PK,
               trim(POL_NUM) :: varchar(25) AS POL_NUM,
               POL_SEQ_NUM :: INTEGER AS POL_SEQ_NUM,
               ORIG_POL_PK :: INTEGER AS ORIG_POL_PK,
               POL_STATE_VRSN :: INTEGER AS POL_STATE_VRSN,
               POL_STATE_EFF_DT :: DATE AS POL_STATE_EFF_DT,
               ACCTG_DT :: DATE AS ACCTG_DT,
               trim(POL_STATE_STAT) :: varchar(10) AS POL_STATE_STAT,
               ROW_XPTN_DT :: DATE AS ROW_XPTN_DT,
               trim(POL_VRSN_TXN_TYP) :: varchar(3) AS POL_VRSN_TXN_TYP,
               trim(ROW_STAT) :: varchar(10) AS ROW_STAT,
               trim(RCD_ACTN_TYP_POL) :: varchar(1) AS RCD_ACTN_TYP_POL,
               DATA_DT :: DATE AS DATA_DT,
               trim(CORP_CD) :: varchar(10) AS CORP_CD,
               trim(CO_CD) :: varchar(10) AS CO_CD,
               SRC_PROD_ID :: INTEGER AS SRC_PROD_ID,
              -- trim(SRC_CD) :: varchar(10) AS SRC_CD,
               SRC_POL_ID :: INTEGER AS SRC_POL_ID,
               CO_ID :: INTEGER AS CO_ID,
               PROD_ID :: INTEGER AS PROD_ID,
             --  SRC_CORP_ID :: INTEGER AS SRC_CORP_ID,
              -- SRC_CO_ID :: INTEGER AS SRC_CO_ID,
               trim(PRCSG_GRP_CD) :: varchar(10) AS PRCSG_GRP_CD,
               trim(PROD_CD) :: varchar(10) AS PROD_CD,
               CUR_TERM_EFF_DT :: DATE AS CUR_TERM_EFF_DT,
               CUR_TERM_XPTN_DT :: DATE AS CUR_TERM_XPTN_DT,
               trim(CNCL_RSN) :: varchar(15) AS CNCL_RSN,
               trim(CNCL_TYP) :: varchar(2) AS CNCL_TYP,
               CNCL_EFF_DT :: DATE AS CNCL_EFF_DT,
               trim(REINST_TYP) :: varchar(10) AS REINST_TYP,
               REINST_EFF_DT :: DATE AS REINST_EFF_DT,
              -- trim(REINST_RSN) :: varchar(50) AS REINST_RSN,
               LAPSE_BEGIN_DT :: DATE AS LAPSE_BEGIN_DT,
               LAPSE_END_DT :: DATE AS LAPSE_END_DT,
               trim(PLN_CD) :: varchar(10) AS PLN_CD,
               trim(DMV_TRANS_TYPE) :: varchar(2) AS DMV_TRANS_TYPE,
               VEH_UNIT_PK :: INTEGER AS VEH_UNIT_PK,
               trim(RCD_ACTN_TYP_VEH) :: varchar(1) AS RCD_ACTN_TYP_VEH,
               ORGL_EFF_DT :: DATE AS ORGL_EFF_DT,
               trim(VEH_TYP_CD) :: varchar(10) AS VEH_TYP_CD,
               trim(VIN) :: varchar(30) AS VIN,
               MODEL_YR :: INTEGER AS MODEL_YR,
               trim(MK) :: varchar(25) AS MK,
               trim(STAT_MAKE_CD) :: varchar(20) AS STAT_MAKE_CD,
               ADDR_PK :: INTEGER AS ADDR_PK,
               trim(PMRY_ADDR_FLG) :: varchar(1) AS PMRY_ADDR_FLG,
               trim(HSE_NUM) :: varchar(10) AS HSE_NUM,
               trim(ADDRESS1) :: varchar(50) AS ADDRESS1,
               trim(ADDRESS2) :: varchar(50) AS ADDRESS2,
               trim(ADDRESS3) :: varchar(50) AS ADDRESS3,
               trim(ADDRESS4) :: varchar(50) AS ADDRESS4,
               trim(CITY) :: varchar(120) AS CITY,
               trim(STATE) :: varchar(2) AS STATE,
               trim(CNTY) :: varchar(60) AS CNTY,
               trim(PSTL_CD) :: varchar(13) AS PSTL_CD,
               trim(ZIP_FIRST5_CD) :: varchar(5) AS ZIP_FIRST5_CD,
               trim(ZIP_LAST4_CD) :: varchar(4) AS ZIP_LAST4_CD,
               INSD_PTY_PK_POL_OWNER :: INTEGER AS INSD_PTY_PK_POL_OWNER,
               SRC_PTY_ID_POL_OWNER :: INTEGER AS SRC_PTY_ID_POL_OWNER,
               trim(NAM_FST_POL_OWNER) :: varchar(40) AS NAM_FST_POL_OWNER,
               trim(NAM_LST_POL_OWNER) :: varchar(40) AS NAM_LST_POL_OWNER,
               trim(NAM_MDL_POL_OWNER) :: varchar(40) AS NAM_MDL_POL_OWNER,
               trim(NAM_SFX_POL_OWNER) :: varchar(10) AS NAM_SFX_POL_OWNER,
               trim(BUS_NAM_POL_OWNER) :: varchar(255) AS BUS_NAM_POL_OWNER,
               trim(PMRY_NAMD_INSD_FLG_POL_OWNER) :: varchar(1) AS PMRY_NAMD_INSD_FLG_POL_OWNER,
               trim(NAMD_INSD_FLG_POL_OWNER) :: varchar(1) AS NAMD_INSD_FLG_POL_OWNER,
               trim(DRVR_FLG_POL_OWNER) :: varchar(1) AS DRVR_FLG_POL_OWNER,
               trim(DRVR_LIC_NUM_POL_OWNER) :: varchar(25) AS DRVR_LIC_NUM_POL_OWNER,
               trim(DRVR_LIC_STATE_POL_OWNER) :: varchar(2) AS DRVR_LIC_STATE_POL_OWNER,
               trim(ASSOCN_TYP_PRMRY_DRVR) :: varchar(10) AS ASSOCN_TYP_PRMRY_DRVR,
               INSD_PTY_PK_PRMRY_DRVR :: INTEGER AS INSD_PTY_PK_PRMRY_DRVR,
               SRC_PTY_ID_PRMRY_DRVR :: INTEGER AS SRC_PTY_ID_PRMRY_DRVR,
               trim(NAM_FST_PRMRY_DRVR) :: varchar(40) AS NAM_FST_PRMRY_DRVR,
               trim(NAM_LST_PRMRY_DRVR) :: varchar(40) AS NAM_LST_PRMRY_DRVR,
               trim(NAM_MDL__PRMRY_DRVR) :: varchar(40) AS NAM_MDL__PRMRY_DRVR,
               trim(NAM_SFX_PRMRY_DRVR) :: varchar(10) AS NAM_SFX_PRMRY_DRVR,
               trim(NAMD_INSD_FLG_PRMRY_DRVR) :: varchar(1) AS NAMD_INSD_FLG_PRMRY_DRVR,
               trim(DRVR_FLG_PRMRY_DRVR) :: varchar(1) AS DRVR_FLG_PRMRY_DRVR,
               trim(DRVR_LIC_NUM_PRMRY_DRVR) :: varchar(25) AS DRVR_LIC_NUM_PRMRY_DRVR,
               trim(DRVR_LIC_STATE__PRMRY_DRVR) :: varchar(2) AS DRVR_LIC_STATE__PRMRY_DRVR,
               BATCH_ID :: INTEGER AS BATCH_ID,
               CREATE_DT :: DATE AS CREATE_DT,
               trim(CREATED_BY) :: varchar(55) AS CREATED_BY,
               BATCH_START_DT :: DATE AS BATCH_START_DT,
               BATCH_END_DT :: DATE AS BATCH_END_DT,
               RPRTG_PERIOD_START_DT :: DATE AS RPRTG_PERIOD_START_DT,
               RPRTG_PERIOD_END_DT :: DATE AS RPRTG_PERIOD_END_DT,
               trim(DMV_VIN) :: varchar(19) AS DMV_VIN,
               trim(DMV_DRIVER_LICENSE_NUMBER) :: varchar(15) AS DMV_DRIVER_LICENSE_NUMBER,
               trim(DMV_MAKE_OF_CAR) :: varchar(5) AS DMV_MAKE_OF_CAR,
               trim(DMV_YEAR_OF_CAR) :: varchar(4) AS DMV_YEAR_OF_CAR,
               trim(DMV_MODEL_OF_CAR) :: varchar(5) AS DMV_MODEL_OF_CAR,
               trim(DMV_INS_COMPANY_CD) :: varchar(4) AS DMV_INS_COMPANY_CD,
               trim(DMV_POLICY_OWNER_STREET_ADDR) :: varchar(30) AS DMV_POLICY_OWNER_STREET_ADDR,
               trim(DMV_POLICY_OWNER_CITY) :: varchar(20) AS DMV_POLICY_OWNER_CITY,
               trim(DMV_POLICY_OWNER_STATE) :: varchar(2) AS DMV_POLICY_OWNER_STATE,
               trim(DMV_POLICY_OWNER_ZIP_CODE) :: varchar(9) AS DMV_POLICY_OWNER_ZIP_CODE,
               trim(DMV_TRANSACTION_TYPE_CODE) :: varchar(1) AS DMV_TRANSACTION_TYPE_CODE,
               trim(DMV_POLICY_EFFECTIVE_DATE) :: varchar(8) AS DMV_POLICY_EFFECTIVE_DATE,
               trim(DMV_POLICY_CANCELLATION_DATE) :: varchar(8) AS DMV_POLICY_CANCELLATION_DATE,
               trim(DMV_DATE_STAMP) :: varchar(8) AS DMV_DATE_STAMP,
               trim(DMV_POLICY_NUMBER) :: varchar(30) AS DMV_POLICY_NUMBER,
               trim(DMV_RESERVED) :: varchar(32) AS DMV_RESERVED,
              -- trim(PRE_CONV_CO_CD) :: varchar(10) AS PRE_CONV_CO_CD
          FROM UPDTRANS
       ) SELECT *
  FROM FINAL_SELECT_S10_NJ_DMV_MTHLY_EXTRACT




  SQL Error [42601]: An unexpected token "rn" was found following "DIM_HST 
    QUALIFY".  Expected tokens may include:  "WHERE".. SQLCODE=-104, SQLSTATE=42601, DRIVER=4.33.31




LEFT JOIN (
    SELECT * FROM (
        SELECT 
            T_POL_PTY_DIM_HST.INSD_PTY_PK AS INSD_PTY_PK,
            T_POL_PTY_DIM_HST.SRC_PTY_ID AS SRC_PTY_ID,
            T_POL_PTY_DIM_HST.NAM_FST AS NAM_FST,
            T_POL_PTY_DIM_HST.NAM_LST AS NAM_LST,
            T_POL_PTY_DIM_HST.NAM_MDL AS NAM_MDL,
            T_POL_PTY_DIM_HST.NAM_SFX AS NAM_SFX,
            T_POL_PTY_DIM_HST.PMRY_NAMD_INSD_FLG AS PMRY_NAMD_INSD_FLG,
            T_POL_PTY_DIM_HST.NAMD_INSD_FLG AS NAMD_INSD_FLG,
            T_POL_PTY_DIM_HST.DRVR_FLG AS DRVR_FLG,
            T_POL_PTY_DIM_HST.DRVR_LIC_NUM AS DRVR_LIC_NUM,
            T_POL_PTY_DIM_HST.DRVR_LIC_STATE AS DRVR_LIC_STATE,
            T_POL_PTY_DIM_HST.POL_PK AS POL_PK,
            ROW_NUMBER() OVER (PARTITION BY POL_PK ORDER BY POL_PK) AS rn
        FROM 
            CDWQA.DWADMN.T_POL_PTY_DIM_HST
    ) AS subquery
    WHERE subquery.rn = 1
) LKP_ALT_LIC_NUM
ON LKP_ALT_LIC_NUM.POL_PK = SQ_S10_NJ_DMV_EXTRACT.POL_PK






LEFT JOIN (
    SELECT * FROM (
        SELECT 
            NCIC_VAL,
            VEH_MAKE_CD,
            ROW_NUMBER() OVER (PARTITION BY VEH_MAKE_CD ORDER BY VEH_MAKE_CD) AS rn
        FROM 
            rdmdev.burrpt.NCIC_CODES_XREF
    ) AS LKP_NCIC_VEH_MAKE_CODE
    WHERE rn = 1
) LKP_NCIC_VEH_MAKE_CODE
ON LKP_NCIC_VEH_MAKE_CODE.VEH_MAKE_CD = SQ_S10_NJ_DMV_EXTRACT.MK


