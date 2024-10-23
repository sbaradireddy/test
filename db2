
WITH SQ_S10_NJ_DMV_EXTRACT AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY CDWQA.DWADMN.T_POL_DIM_HST.POL_PK) AS source_record_id,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.STATE,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.CNTY,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.PSTL_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.PRE_CONV_CO_CD,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.ADDR_PK,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.PMRY_ADDR_FLG,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.HSE_NUM,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_PK,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_NUM,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_SEQ_NUM,
        CDWQA.DWADMN.T_POL_DIM_HST.LAPSE_BEGIN_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.LAPSE_END_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.DATA_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.PROD_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.PLN_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.CNCL_TYP,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_VRSN_TXN_TYP,
        CDWQA.DWADMN.T_POL_DIM_HST.ROW_STAT,
        CDWQA.DWADMN.T_POL_DIM_HST.CUR_TERM_EFF_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.CUR_TERM_XPTN_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.CNCL_RSN,
        CDWQA.DWADMN.T_POL_DIM_HST.RCD_ACTN_TYP,
        CDWQA.DWADMN.T_POL_FLG_DIM_HST.PRIME_CONV_FLG,
        CDWQA.DWADMN.T_POL_FLG_DIM_HST.PRIME_POL_FLG,
        CDWQA.DWADMN.T_POL_CT_DIM_HST.VEH_CT,
        CDWQA.DWADMN.T_POL_DIM_HST.PRCSG_GRP_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_STATE_STAT,
        CDWQA.DWADMN.T_POL_DIM_HST.SRC_POL_ID,
        CDWQA.DWADMN.T_POL_DIM_HST.CO_ID,
        CDWQA.DWADMN.T_POL_DIM_HST.PROD_ID,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_STATE_VRSN,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_STATE_EFF_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.ROW_XPTN_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.CORP_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.CO_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.SRC_PROD_ID,
        CDWQA.DWADMN.T_POL_DIM_HST.SRC_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.CNCL_EFF_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.REINST_EFF_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.ACCTG_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.REINST_TYP,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.DRVR_LIC_NUM,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.DRVR_LIC_STATE,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.INSD_PTY_PK,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.SRC_PTY_ID,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.NAM_FST,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.NAM_LST,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.NAM_MDL,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.NAM_SFX,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.BUS_NAM,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.PMRY_NAMD_INSD_FLG,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.NAMD_INSD_FLG,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.DRVR_FLG,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.STAT_MAKE_CD,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.MODEL_YR,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.MK,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.VEH_UNIT_PK,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.RCD_ACTN_TYP AS RCD_ACTN_TYP_veh,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.ORGL_EFF_DT,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.VEH_TYP_CD,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.VIN,
        CDWQA.DWADMN.T_CO_DIM.SRC_CORP_ID,
        CDWQA.DWADMN.T_CO_DIM.SRC_CO_ID,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.ADDRESS1,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.ADDRESS2,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.ADDRESS3,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.ADDRESS4,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.CITY
    FROM
          {{ source('cdw_dwadmn_source', 't_pol_pty_dim_hst') }},
          {{ source('cdw_dwadmn_source', 't_co_dim') }},  
          {{ source('cdw_dwadmn_source', 't_pol_addr_dim_hst') }},
          {{ source('cdw_dwadmn_source', 't_pol_flg_dim_hst') }},
          {{ source('cdw_dwadmn_source', 't_pol_ct_dim_hst') }},
          {{ source('cdw_dwadmn_source', 't_pol_veh_unit_dim_hst') }},
          {{ source('cdw_dwadmn_source', 't_pol_dim_hst') }}
    WHERE
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.POL_PK = CDWQA.DWADMN.T_POL_DIM_HST.POL_PK
        AND CDWQA.DWADMN.T_POL_PTY_DIM_HST.POL_PK = CDWQA.DWADMN.T_POL_DIM_HST.POL_PK
        AND CDWQA.DWADMN.T_POL_FLG_DIM_HST.POL_PK = CDWQA.DWADMN.T_POL_DIM_HST.POL_PK
        AND CDWQA.DWADMN.T_POL_CT_DIM_HST.POL_PK = CDWQA.DWADMN.T_POL_DIM_HST.POL_PK
        AND CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.POL_PK = CDWQA.DWADMN.T_POL_DIM_HST.POL_PK
        AND CDWQA.DWADMN.T_POL_DIM_HST.CO_ID = CDWQA.DWADMN.T_CO_DIM.CO_ID
        AND RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.PRCSG_GRP_CD) IN ('HP', 'PL')
        AND RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.STATE_CD) = 'NJ'
        AND RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.CO_CD) IN ('HPPREF', 'HPPROP', 'HPPROP2', 'HPSIC', 'PAIPTWIN', 'PIC', 'PSIA', 'TCTIPU', 'TCTIPU2', 'ALN_HPCIC', 'ALN_TEACH', 'ALN_PSIA', 'ALN_PIC')
        AND RTRIM(CDWQA.DWADMN.T
        AND RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.CORP_CD) IN ('HIGHPOINT', 'Palisades')
        AND (
            (RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.PROD_CD) IN ('PA', 'PAIP'))
            OR (RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.PROD_CD) = 'PIC_NEW_CA' 
                AND LTRIM(RTRIM(CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.VEH_TYP_CD)) = 'LMO'
                AND CDWQA.DWADMN.T_POL_CT_DIM_HST.VEH_CT <= 5)
        )
        AND CDWQA.DWADMN.T_POL_DIM_HST.RCD_ACTN_TYP <> 'D'
        AND (
            (CDWQA.DWADMN.T_POL_DIM_HST.ACCTG_DT <= '{{BATCH_END_DT}}'
            AND CDWQA.DWADMN.T_POL_DIM_HST.POL_STATE_EFF_DT <= '{{RPRTG_PERIOD_END_DT}}'
            AND CDWQA.DWADMN.T_POL_DIM_HST.POL_STATE_EFF_DT >= '{{RPRTG_PERIOD_START_DT}}')
            OR (CDWQA.DWADMN.T_POL_DIM_HST.ACCTG_DT <= '{{BATCH_END_DT}}'
            AND CDWQA.DWADMN.T_POL_DIM_HST.ACCTG_DT >= '{{BATCH_START_DT}}'
            AND CDWQA.DWADMN.T_POL_DIM_HST.POL_STATE_EFF_DT < '{{RPRTG_PERIOD_START_DT}}')
        )
        AND ((CDWQA.DWADMN.T_POL_DIM_HST.POL_VRSN_TXN_TYP = 'NB'
            AND (CDWQA.DWADMN.T_POL_DIM_HST.CONV_POL = 'N' OR CDWQA.DWADMN.T_POL_DIM_HST.CONV_POL IS NULL))
        OR (CDWQA.DWADMN.T_POL_FLG_DIM_HST.PRIME_CONV_FLG = 'Y'
            AND CDWQA.DWADMN.T_POL_DIM_HST.CO_CD = 'ALN_PIC'
            AND CDWQA.DWADMN.T_POL_DIM_HST.POL_VRSN_TXN_TYP = 'RN')
        OR (CDWQA.DWADMN.T_POL_FLG_DIM_HST.PRIME_CONV_FLG = 'Y'
            AND CDWQA.DWADMN.T_POL_DIM_HST.CO_CD = 'ALN_PSIA'
            AND CDWQA.DWADMN.T_POL_DIM_HST.POL_VRSN_TXN_TYP = 'RN'
            AND CDWQA.DWADMN.T_POL_DIM_HST.PRE_CONV_CO_CD = 'PIC')
        OR (CDWQA.DWADMN.T_POL_DIM_HST.POL_VRSN_TXN_TYP = 'RN'
            AND CDWQA.DWADMN.T_POL_DIM_HST.ICT_DT = CDWQA.DWADMN.T_POL_DIM_HST.CUR_TERM_EFF_DT
            AND CDWQA.DWADMN.T_POL_CT_DIM_HST.ICT_CT = 1)
        OR (SUBSTR(CDWQA.DWADMN.T_POL_DIM_HST.POL_VRSN_TXN_TYP, 1, 2) = 'EN'
            AND CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.RCD_ACTN_TYP = 'A'
            AND CDWQA.DWADMN.T_POL_DIM_HST.POL_STATE_EFF_DT >= CDWQA.DWADMN.T_POL_DIM_HST.CUR_TERM_EFF_DT)
        OR (RTRIM(LTRIM(CDWQA.DWADMN.T_POL_DIM_HST.POL_VRSN_TXN_TYP)) IN ('CN', 'CNX')
            AND RTRIM(LTRIM(CDWQA.DWADMN.T_POL_DIM_HST.CNCL_RSN)) IN ('NONPAY', 'NP', 'NP_CR', 'NP_PF'))
        OR (SUBSTR(CDWQA.DWADMN.T_POL_DIM_HST.POL_VRSN_TXN_TYP, 1, 2) = 'RS'
            AND RTRIM(LTRIM(CDWQA.DWADMN.T_POL_DIM_HST.REINST_TYP)) = 'WITH LAPSE'
            AND RTRIM(LTRIM(CDWQA.DWADMN.T_POL_DIM_HST.CNCL_RSN)) IN ('NONPAY', 'NP', 'NP_CR', 'NP_PF'))
        OR (SUBSTR(CDWQA.DWADMN.T_POL_DIM_HST.POL_VRSN_TXN_TYP, 1, 2) = 'RS'
            AND RTRIM(LTRIM(CDWQA.DWADMN.T_POL_DIM_HST.REINST_TYP)) = 'REINSTATE'
            AND RTRIM(LTRIM(CDWQA.DWADMN.T_POL_DIM_HST.CNCL_RSN)) IN ('NONPAY', 'NP', 'NP_CR', 'NP_PF')
            AND CDWQA.DWADMN.T_POL_DIM_HST.ROW_STAT = 'C'))
        )
        AND CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.VEH_TYP_CD IN ('AQ', 'CL', 'CV', 'PP', 'PPA', 'PPP', 'LMO', 'VAN')
        AND CDWQA.DWADMN.T_POL_PTY_DIM_HST.PMRY_NAMD_INSD_FLG = 'Y'
        AND CDWQA.DWADMN.T_POL_ADDR_DIM_HST.PMRY_ADDR_FLG = 'Y'
        AND (CDWQA.DWADMN.T_POL_FLG_DIM_HST.MOTORCYCLE_POL_FLG != 'Y' OR CDWQA.DWADMN.T_POL_FLG_DIM_HST.MOTORCYCLE_POL_FLG IS NULL)
        AND NOT EXISTS (
            SELECT *
            FROM {{ source('rrm_source', 't_ess_smoketest_policies') }} AS T_ESS_SMOKETEST_POLICIES
            WHERE T_ESS_SMOKETEST_POLICIES.STATE_CD = CDWQA.DWADMN.T_POL_DIM_HST.STATE_CD
            AND T_ESS_SMOKETEST_POLICIES.POL_NUM = CDWQA.DWADMN.T_POL_DIM_HST.POL_NUM
        )
)















WITH SQ_S10_NJ_DMV_EXTRACT AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY CDWQA.DWADMN.T_POL_DIM_HST.POL_PK) AS source_record_id,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.STATE,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.CNTY,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.PSTL_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.PRE_CONV_CO_CD,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.ADDR_PK,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.PMRY_ADDR_FLG,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.HSE_NUM,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_PK,  -- Fully qualified
        CDWQA.DWADMN.T_POL_DIM_HST.POL_NUM,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_SEQ_NUM,
        CDWQA.DWADMN.T_POL_DIM_HST.LAPSE_BEGIN_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.LAPSE_END_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.DATA_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.PROD_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.PLN_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.CNCL_TYP,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_VRSN_TXN_TYP,
        CDWQA.DWADMN.T_POL_DIM_HST.ROW_STAT,
        CDWQA.DWADMN.T_POL_DIM_HST.CUR_TERM_EFF_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.CUR_TERM_XPTN_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.CNCL_RSN,
        CDWQA.DWADMN.T_POL_DIM_HST.RCD_ACTN_TYP,
        CDWQA.DWADMN.T_POL_FLG_DIM_HST.PRIME_CONV_FLG,
        CDWQA.DWADMN.T_POL_FLG_DIM_HST.PRIME_POL_FLG,
        CDWQA.DWADMN.T_POL_CT_DIM_HST.VEH_CT,
        CDWQA.DWADMN.T_POL_DIM_HST.PRCSG_GRP_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_STATE_STAT,
        CDWQA.DWADMN.T_POL_DIM_HST.SRC_POL_ID,
        CDWQA.DWADMN.T_POL_DIM_HST.CO_ID,
        CDWQA.DWADMN.T_POL_DIM_HST.PROD_ID,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_STATE_VRSN,
        CDWQA.DWADMN.T_POL_DIM_HST.POL_STATE_EFF_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.ROW_XPTN_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.CORP_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.CO_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.SRC_PROD_ID,
        CDWQA.DWADMN.T_POL_DIM_HST.SRC_CD,
        CDWQA.DWADMN.T_POL_DIM_HST.CNCL_EFF_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.REINST_EFF_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.ACCTG_DT,
        CDWQA.DWADMN.T_POL_DIM_HST.REINST_TYP,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.DRVR_LIC_NUM,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.DRVR_LIC_STATE,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.INSD_PTY_PK,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.SRC_PTY_ID,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.NAM_FST,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.NAM_LST,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.NAM_MDL,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.NAM_SFX,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.BUS_NAM,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.PMRY_NAMD_INSD_FLG,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.NAMD_INSD_FLG,
        CDWQA.DWADMN.T_POL_PTY_DIM_HST.DRVR_FLG,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.STAT_MAKE_CD,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.MODEL_YR,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.MK,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.VEH_UNIT_PK,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.RCD_ACTN_TYP AS RCD_ACTN_TYP_veh,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.ORGL_EFF_DT,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.VEH_TYP_CD,
        CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.VIN,
        CDWQA.DWADMN.T_CO_DIM.SRC_CORP_ID,
        CDWQA.DWADMN.T_CO_DIM.SRC_CO_ID,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.ADDRESS1,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.ADDRESS2,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.ADDRESS3,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.ADDRESS4,
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.CITY
    FROM
          {{ source('cdw_dwadmn_source', 't_pol_pty_dim_hst') }} T_POL_PTY_DIM_HST, 
          {{ source('cdw_dwadmn_source', 't_co_dim') }} T_CO_DIM,  
          {{ source('cdw_dwadmn_source', 't_pol_addr_dim_hst') }} T_POL_ADDR_DIM_HST,
          {{ source('cdw_dwadmn_source', 't_pol_flg_dim_hst') }} T_POL_FLG_DIM_HST,
          {{ source('cdw_dwadmn_source', 't_pol_ct_dim_hst') }} T_POL_CT_DIM_HST,
          {{ source('cdw_dwadmn_source', 't_pol_veh_unit_dim_hst') }} T_POL_VEH_UNIT_DIM_HST,
          {{ source('cdw_dwadmn_source', 't_pol_dim_hst') }} T_POL_DIM_HST
    WHERE
        CDWQA.DWADMN.T_POL_ADDR_DIM_HST.POL_PK = CDWQA.DWADMN.T_POL_DIM_HST.POL_PK
        AND CDWQA.DWADMN.T_POL_PTY_DIM_HST.POL_PK = CDWQA.DWADMN.T_POL_DIM_HST.POL_PK
        AND CDWQA.DWADMN.T_POL_FLG_DIM_HST.POL_PK = CDWQA.DWADMN.T_POL_DIM_HST.POL_PK
        AND CDWQA.DWADMN.T_POL_CT_DIM_HST.POL_PK = CDWQA.DWADMN.T_POL_DIM_HST.POL_PK
        AND CDWQA.DWADMN.T_POL_VEH_UNIT_DIM_HST.POL_PK = CDWQA.DWADMN.T_POL_DIM_HST.POL_PK
        AND CDWQA.DWADMN.T_POL_DIM_HST.CO_ID = CDWQA.DWADMN.T_CO_DIM.CO_ID
        AND RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.PRCSG_GRP_CD) IN ('HP', 'PL')
        AND RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.STATE_CD) = 'NJ'
        AND RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.CO_CD) IN ('HPPREF', 'HPPROP', 'HPPROP2', 'HPSIC', 'PAIPTWIN', 'PIC', 'PSIA', 'TCTIPU', 'TCTIPU2', 'ALN_HPCIC', 'ALN_TEACH', 'ALN_PSIA', 'ALN_PIC')
        AND RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.CORP_CD) IN ('HIGHPOINT', 'Palisades')
        AND (
            (RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.PROD_CD) IN ('PA', 'PAIP'))
            OR (RTRIM(CDWQA.DWADMN.T_POL_DIM_HST.PROD_CD) = 'PIC_NEW_CA' 
			AND LTRIM(RTRIM(T_POL_VEH_UNIT_DIM_HST.VEH_TYP_CD)) = 'LMO'
				AND T_POL_CT_DIM_HST.VEH_CT <= 5)
        )
	AND T_POL_DIM_HST.RCD_ACTN_TYP <> 'D'
	AND (
            (T_POL_DIM_HST.ACCTG_DT <= '{{BATCH_END_DT}}'
		AND T_POL_DIM_HST.POL_STATE_EFF_DT <= '{{RPRTG_PERIOD_END_DT}}'
		AND T_POL_DIM_HST.POL_STATE_EFF_DT >= '{{RPRTG_PERIOD_START_DT}}'
	OR (T_POL_DIM_HST.ACCTG_DT <= '{{BATCH_END_DT}}'
		AND T_POL_DIM_HST.ACCTG_DT >= '{{BATCH_START_DT}}'
		AND T_POL_DIM_HST.POL_STATE_EFF_DT < '{{RPRTG_PERIOD_START_DT}}')
        )
	AND ((T_POL_DIM_HST.Pol_Vrsn_Txn_Typ = 'NB'
		AND (T_POL_DIM_HST.conv_pol = 'N'
			OR T_POL_DIM_HST.conv_pol IS NULL))
	OR (T_POL_FLG_DIM_HST.PRIME_CONV_FLG = 'Y'
		AND T_POL_DIM_HST.CO_CD = 'ALN_PIC'
		AND T_POL_DIM_HST.POL_VRSN_TXN_TYP = 'RN')
	OR (T_POL_FLG_DIM_HST.PRIME_CONV_FLG = 'Y'
		AND T_POL_DIM_HST.CO_CD = 'ALN_PSIA'
		AND T_POL_DIM_HST.POL_VRSN_TXN_TYP = 'RN'
		AND T_POL_DIM_HST. PRE_CONV_CO_CD = 'PIC')
	OR (T_POL_DIM_HST.Pol_Vrsn_Txn_Typ = 'RN'
		AND T_POL_DIM_HST.ict_dt = T_POL_DIM_HST.cur_term_eff_dt
		AND T_POL_CT_DIM_HST.ict_ct = 1)
	OR (SUBSTR(T_POL_DIM_HST.POL_VRSN_TXN_TYP, 1, 2) = 'EN'
		AND T_POL_VEH_UNIT_DIM_HST.RCD_ACTN_TYP = 'A'
		AND T_POL_DIM_HST.POL_STATE_EFF_DT >= T_POL_DIM_HST.CUR_TERM_EFF_DT)
	OR (RTRIM(LTRIM(T_POL_DIM_HST.POL_VRSN_TXN_TYP)) IN ('CN', 'CNX')
		AND RTRIM(LTRIM(T_POL_DIM_HST.CNCL_RSN)) IN ('NONPAY', 'NP', 'NP_CR', 'NP_PF'))
		OR (SUBSTR(T_POL_DIM_HST.POL_VRSN_TXN_TYP, 1, 2) = 'RS'
			AND RTRIM(LTRIM(T_POL_DIM_HST.REINST_TYP)) = 'WITH LAPSE'
				AND RTRIM(LTRIM(T_POL_DIM_HST.CNCL_RSN)) IN ('NONPAY', 'NP', 'NP_CR', 'NP_PF'))
			OR (SUBSTR(T_POL_DIM_HST.POL_VRSN_TXN_TYP, 1, 2) = 'RS'
				AND RTRIM(LTRIM(T_POL_DIM_HST.REINST_TYP)) = 'REINSTATE'
					AND RTRIM(LTRIM(T_POL_DIM_HST.CNCL_RSN)) IN ('NONPAY', 'NP', 'NP_CR', 'NP_PF')
						AND T_POL_DIM_HST.ROW_STAT = 'C'))
	AND T_POL_VEH_UNIT_DIM_HST.Veh_Typ_Cd IN ('AQ', 'CL', 'CV', 'PP', 'PPA', 'PPP', 'LMO', 'VAN')
	AND T_POL_PTY_DIM_HST.Pmry_Namd_Insd_Flg = 'Y'
	AND T_POL_ADDR_DIM_HST.Pmry_Addr_Flg = 'Y'
	AND (T_POL_FLG_DIM_HST.MOTORCYCLE_POL_FLG != 'Y'
		OR T_POL_FLG_DIM_HST.MOTORCYCLE_POL_FLG IS NULL)
	AND NOT EXISTS (
	SELECT
		*
	FROM
		{{ source('rrm_source', 't_ess_smoketest_policies') }} T_ESS_SMOKETEST_POLICIES
	WHERE
		T_ESS_SMOKETEST_POLICIES.STATE_CD = T_POL_DIM_HST.STATE_CD
		AND T_ESS_SMOKETEST_POLICIES.POL_NUM = T_POL_DIM_HST.POL_NUM
               )
 )
,
