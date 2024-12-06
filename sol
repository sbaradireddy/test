Exp_Inputs_before_agg_DW as (
        -- writting query for expression function
SELECT Exp_Input_DW.source_record_id,
               Exp_Input_DW.S10_ISO_PLOTA_EXTRACT_ID,
               Exp_Input_DW.POL_STAT_PREM_PK,             
               Exp_Input_DW.IMPACT_RESIST_GLASS_FLG,
               Exp_Input_DW.HURRICANE_SHUTTERS_FLG,
               Exp_Input_DW.ISO_PROTECTIVE_DEVICES,
               Exp_Input_DW.ISO_LOSS_HISTORY_CD,
               Exp_Input_DW.ISO_ROOF_SURFACE_LOSS_CD,
               Exp_Input_DW.ISO_ROOF_INSTALL_YR_CD,
               Exp_Input_DW.ISO_RISK_IND_CD,
               Exp_Input_DW.ISO_RISK_IND_CD_II,
               LKP_WIND_HAIL_DED_PCT.NAMD_STORM_DDCTBL_OPT AS lkp_NAMD_STORM_DDCTBL_OPT,
               LKP_MOLD_II_LIMITS.LIMIT_AMT1 AS lkp_LIMIT_AMT1,
               LKP_MOLD_II_LIMITS.LIMIT_AMT2 AS lkp_LIMIT_AMT2,
               Lkp_Max_Subm_Id1.ISO_BURRPT_QTR_SUBM_ID,
               Lkp_POL_DEDUCTIBLE.DED_AMT1 AS lkp_DED_AMT1,
               LKP_MOLD_LIMITS.LIMIT_AMT1 AS LKP_MOLD_LIMITS_lkp_LIMIT_AMT1,
               LKP_MOLD_LIMITS.LIMIT_AMT2 AS LKP_MOLD_LIMITS_lkp_LIMIT_AMT2,
               Lkp_OIL_SPILL_COV_Exists.PREMIUM_SOURCE_C AS lkp_PREMIUM_SOURCE_C,
               Lkp_SUPER_PREF_CRD_COV_Exists.POL_STATE_EFF_DT AS lkp_POL_STATE_EFF_DT,
               Lkp_STATUS_Cov_Cd_Count.STATUS_COV_COUNT,
               LKP_PROTECTION_DEVICES.PTCTV_DVC_BRGLR_ALM_TYP AS lkp_PTCTV_DVC_BRGLR_ALM_TYP,
               LKP_PROTECTION_DEVICES.PTCTV_DVC_FIRE_ALM_TYP AS lkp_PTCTV_DVC_FIRE_ALM_TYP,
               LKP_PROTECTION_DEVICES.PTCTV_DVC_SPRNKLR_TYP AS lkp_PTCTV_DVC_SPRNKLR_TYP
          FROM Exp_Input_DW Exp_Input_DW
          left join -- writting query for lookup function
 (
                select NAMD_STORM_DDCTBL_OPT,
                       POL_PK,
                       ORIG_POL_PK,
                       row_number() over(partition by POL_PK, ORIG_POL_PK order by POL_PK, ORIG_POL_PK) as rn -- Please review the order by columns

                  FROM S10_ISO_PLOTA_EXTRACT_NE
                 where RTRIM(LTRIM(PREMIUM_SOURCE_C)) = 'NMD_STORM' qualify rn = 1
               ) LKP_WIND_HAIL_DED_PCT
            on LKP_WIND_HAIL_DED_PCT.POL_PK=Exp_Input_DW.POL_PK
           AND LKP_WIND_HAIL_DED_PCT.ORIG_POL_PK=Exp_Input_DW.ORIG_POL_PK
          left join -- writting query for lookup function
 (
                select LIMIT_AMT1,
                       LIMIT_AMT2,
                       POL_PK,
                       ORIG_POL_PK,
                       row_number() over(partition by POL_PK, ORIG_POL_PK order by POL_PK, ORIG_POL_PK) as rn -- Please review the order by columns

                  FROM S10_ISO_PLOTA_EXTRACT_NE
                 where RTRIM(LTRIM(PREMIUM_SOURCE_C)) = 'MOLD_II' qualify rn = 1
               ) LKP_MOLD_II_LIMITS
            on LKP_MOLD_II_LIMITS.POL_PK=Exp_Input_DW.POL_PK
           AND LKP_MOLD_II_LIMITS.ORIG_POL_PK=Exp_Input_DW.ORIG_POL_PK
          left join -- writting query for lookup function
 (
                SELECT MAX(BURRPT.ISO_BURRPT_PLOTA_PREM_HO_GEN_SUB_HIST_NE.ISO_BURRPT_QTR_SUBM_ID) as ISO_BURRPT_QTR_SUBM_ID,
                      -- BURRPT.ISO_BURRPT_PLOTA_PREM_HO_GEN_SUB_HIST_NE.RPRTG_PERIOD_END_DT as RPRTG_PERIOD_END_DT,
                       row_number() over(partition by RPRTG_PERIOD_END_DT order by RPRTG_PERIOD_END_DT) as rn -- Please review the order by columns

                  FROM BURRPT.ISO_BURRPT_PLOTA_PREM_HO_GEN_SUB_HIST_NE --qualify rn = 1
                 --Group by BURRPT.ISO_BURRPT_PLOTA_PREM_HO_GEN_SUB_HIST_NE.RPRTG_PERIOD_END_DT
                 -- ORDER BY RPRTG_PERIOD_END_DT,
                 --          ISO_BURRPT_QTR_SUBM_ID DESC
               ) Lkp_Max_Subm_Id1
          --  on Lkp_Max_Subm_Id1.RPRTG_PERIOD_END_DT = Exp_Input_DW.RPRTG_PERIOD_END_DT
          left join -- writting query for lookup function
 (
                SELECT S10_ISO_PLOTA_EXTRACT_NE.DED_AMT1 as DED_AMT1,
                       S10_ISO_PLOTA_EXTRACT_NE.POL_PK as POL_PK,
                       S10_ISO_PLOTA_EXTRACT_NE.ORIG_POL_PK as ORIG_POL_PK, row_number() over(partition by POL_PK, ORIG_POL_PK order by POL_PK, ORIG_POL_PK) as rn -- Please review the order by columns

                  FROM S10_ISO_PLOTA_EXTRACT_NE
                 WHERE LTRIM(RTRIM(S10_ISO_PLOTA_EXTRACT_NE.PREMIUM_SOURCE_C)) = 'POL_DED' qualify rn = 1
               ) Lkp_POL_DEDUCTIBLE
            on Lkp_POL_DEDUCTIBLE.POL_PK=Exp_Input_DW.POL_PK
           AND Lkp_POL_DEDUCTIBLE.ORIG_POL_PK=Exp_Input_DW.ORIG_POL_PK
          left join -- writting query for lookup function
 (
                select LIMIT_AMT1,
                       LIMIT_AMT2,
                       POL_PK,
                       ORIG_POL_PK,
                       row_number() over(partition by POL_PK, ORIG_POL_PK order by POL_PK, ORIG_POL_PK) as rn -- Please review the order by columns

                  FROM S10_ISO_PLOTA_EXTRACT_NE
                 where RTRIM(LTRIM(PREMIUM_SOURCE_C)) = 'MOLD'
                    or RTRIM(LTRIM(PREMIUM_SOURCE_C)) = 'MOLD_II' qualify rn = 1
               ) LKP_MOLD_LIMITS
            on LKP_MOLD_LIMITS.POL_PK=Exp_Input_DW.POL_PK
           AND LKP_MOLD_LIMITS.ORIG_POL_PK=Exp_Input_DW.ORIG_POL_PK
          left join -- writting query for lookup function
 (
                SELECT S10_ISO_PLOTA_EXTRACT_NE.POL_STATE_EFF_DT as POL_STATE_EFF_DT,
                       S10_ISO_PLOTA_EXTRACT_NE.PREMIUM_SOURCE_C as PREMIUM_SOURCE_C,
                       S10_ISO_PLOTA_EXTRACT_NE.POL_PK as POL_PK,
                       S10_ISO_PLOTA_EXTRACT_NE.ORIG_POL_PK as ORIG_POL_PK,
                       row_number() over(partition by POL_PK, ORIG_POL_PK order by POL_PK, ORIG_POL_PK) as rn -- Please review the order by columns

                  FROM S10_ISO_PLOTA_EXTRACT_NE
                 where LTRIM(RTRIM(PREMIUM_SOURCE_C)) in ('OIL_SPILL')
                   AND PREM_WRIT_M <>0 qualify rn = 1
               ) Lkp_OIL_SPILL_COV_Exists
            on Lkp_OIL_SPILL_COV_Exists.POL_PK=Exp_Input_DW.POL_PK
           AND Lkp_OIL_SPILL_COV_Exists.ORIG_POL_PK=Exp_Input_DW.ORIG_POL_PK
          left join -- writting query for lookup function
 (
                SELECT h.POL_PK as POL_PK,
                       h.ORIG_POL_PK as ORIG_POL_PK,
                       h.POL_STATE_EFF_DT as POL_STATE_EFF_DT,
                       row_number() over(partition by POL_PK, ORIG_POL_PK order by POL_PK, ORIG_POL_PK) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h
                 where LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) in ('PREF_CRD','SUPER_PREF')
                   AND PREM_WRIT_M <>0 qualify rn = 1
               ) Lkp_SUPER_PREF_CRD_COV_Exists
            on Lkp_SUPER_PREF_CRD_COV_Exists.POL_PK=Exp_Input_DW.POL_PK
           AND Lkp_SUPER_PREF_CRD_COV_Exists.ORIG_POL_PK=Exp_Input_DW.ORIG_POL_PK
          left join -- writting query for lookup function
 (
                select COUNT(h.POL_PK) as STATUS_COV_COUNT,
                       h.POL_PK as POL_PK,
                       h.ORIG_POL_PK as ORIG_POL_PK,
                       row_number() over(partition by POL_PK, ORIG_POL_PK order by POL_PK, ORIG_POL_PK) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h
                 where RTRIM(LTRIM(h.PREMIUM_SOURCE_C)) in ('BUILD_ADDC','BUS_PUR_C','COVA_SPCL','FARM_LIABC','INC_LMT_C','INC_OCP_OC','INC_OCPNCY','LOSS_ASMTC','PI','RES_REN_C','RES_REN_TH','SINK_COV','SPCL_CMPTR','STRUC_RENC','THREE_FAM') --qualify rn = 1
                -- GROUP BY h.POL_PK,
                          --ORIG_POL_PK
                 -- ORDER BY h.POL_PK ASC,
                 --          ORIG_POL_PK ASC
               ) Lkp_STATUS_Cov_Cd_Count
            on Lkp_STATUS_Cov_Cd_Count.POL_PK=Exp_Input_DW.POL_PK
           AND Lkp_STATUS_Cov_Cd_Count.ORIG_POL_PK=Exp_Input_DW.ORIG_POL_PK
          left join -- writting query for lookup function
 (
                SELECT S10_ISO_PLOTA_EXTRACT_NE.POL_PK as POL_PK,
                       S10_ISO_PLOTA_EXTRACT_NE.ORIG_POL_PK as ORIG_POL_PK,
                       LTRIM(RTRIM(S10_ISO_PLOTA_EXTRACT_NE.PTCTV_DVC_BRGLR_ALM_TYP)) as PTCTV_DVC_BRGLR_ALM_TYP,
                       LTRIM(RTRIM(S10_ISO_PLOTA_EXTRACT_NE.PTCTV_DVC_FIRE_ALM_TYP)) as PTCTV_DVC_FIRE_ALM_TYP,
                       LTRIM(RTRIM(S10_ISO_PLOTA_EXTRACT_NE.PTCTV_DVC_SPRNKLR_TYP)) as PTCTV_DVC_SPRNKLR_TYP,
                       row_number() over(partition by POL_PK, ORIG_POL_PK order by POL_PK, ORIG_POL_PK) as rn -- Please review the order by columns

                  FROM BURRPT.S10_ISO_PLOTA_EXTRACT_NE S10_ISO_PLOTA_EXTRACT_NE
                 WHERE RTRIM(LTRIM(PREMIUM_SOURCE_C))='PROT_DEV' qualify rn = 1
               ) LKP_PROTECTION_DEVICES
            on LKP_PROTECTION_DEVICES.POL_PK=Exp_Input_DW.POL_PK
           AND LKP_PROTECTION_DEVICES.ORIG_POL_PK=Exp_Input_DW.ORIG_POL_PK
       ) 
