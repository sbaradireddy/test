 FROM Exp_Inputs_before_agg_DW as T0 -- writting query for unconnected lookup function

          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp10
            on lkp10.POL_PK=T0.in_POL_PK
           AND lkp10.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp10.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp11
            on lkp11.POL_PK=T0.in_POL_PK
           AND lkp11.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp11.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp12
            on lkp12.POL_PK=T0.in_POL_PK
           AND lkp12.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp12.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp13
            on lkp13.POL_PK=T0.in_POL_PK
           AND lkp13.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp13.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp14
            on lkp14.POL_PK=T0.in_POL_PK
           AND lkp14.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp14.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp15
            on lkp15.POL_PK=T0.in_POL_PK
           AND lkp15.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp15.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp16
            on lkp16.POL_PK=T0.in_POL_PK
           AND lkp16.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp16.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp17
            on lkp17.POL_PK=T0.in_POL_PK
           AND lkp17.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp17.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp18
            on lkp18.POL_PK=T0.in_POL_PK
           AND lkp18.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp18.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp19
            on lkp19.POL_PK=T0.in_POL_PK
           AND lkp19.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp19.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp110
            on lkp110.POL_PK=T0.in_POL_PK
           AND lkp110.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp110.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp111
            on lkp111.POL_PK=T0.in_POL_PK
           AND lkp111.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp111.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp112
            on lkp112.POL_PK=T0.in_POL_PK
           AND lkp112.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp112.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp113
            on lkp113.POL_PK=T0.in_POL_PK
           AND lkp113.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp113.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp114
            on lkp114.POL_PK=T0.in_POL_PK
           AND lkp114.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp114.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp115
            on lkp115.POL_PK=T0.in_POL_PK
           AND lkp115.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp115.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp116
            on lkp116.POL_PK=T0.in_POL_PK
           AND lkp116.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp116.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp117
            on lkp117.POL_PK=T0.in_POL_PK
           AND lkp117.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp117.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp118
            on lkp118.POL_PK=T0.in_POL_PK
           AND lkp118.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp118.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp119
            on lkp119.POL_PK=T0.in_POL_PK
           AND lkp119.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp119.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp120
            on lkp120.POL_PK=T0.in_POL_PK
           AND lkp120.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp120.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp121
            on lkp121.POL_PK=T0.in_POL_PK
           AND lkp121.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp121.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp122
            on lkp122.POL_PK=T0.in_POL_PK
           AND lkp122.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp122.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp123
            on lkp123.POL_PK=T0.in_POL_PK
           AND lkp123.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp123.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp124
            on lkp124.POL_PK=T0.in_POL_PK
           AND lkp124.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp124.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select COUNT(*) as COV_COUNT ,
                       h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.PREMIUM_SOURCE_C
               ) lkp125
            on lkp125.POL_PK=T0.in_POL_PK
           AND lkp125.ORIG_POL_PK=T0.in_ORIG_POL_PK
           AND lkp125.PREMIUM_SOURCE_C=T0.in_PREMIUM_SOURCE_C
       ) ,
EXP_HO_SPECIFIC_ISO_PREM_FIELDS_DW as (
        -- writting query for expression function
SELECT T1.UNIT_TYPE AS IN_DWELLING_FORM_CODE,
               T1.DWLG_USE_OCPNY AS IN_DF_DWELLING_USE_OCCUP_CD,
               T1.YR_BLT AS IN_CONSTRUCTION_YEAR,
               T1.PREMIUM_SOURCE_C AS IN_COV_CD,
               T1.STRCT_TYP AS IN_TOWN_OR_ROW_HOUSE_IND,
               T1.NUM_OF_FMLYS AS IN_NBR_FAMILIES_QTY,
               T1.LMT_1_AMT AS IN_LIMIT_AMT,
               T1.SCNDY_HME_FLG AS IN_SECONDARY_DWELLING_IND,
               T1.SCNDY_HME_FLG AS IN_SEASONAL_DWELLING_IND,
               T1.RISK_UNIT_LEAD_XCLSN_FLG AS IN_LEAD_EXCL_ALL_UNITS_DLEAD_IND,
               T1.STATE_CODE AS IN_PLCY_ST_CD,
               T1.PREMIUM_SOURCE_C AS IN_PREM_COV_CD,
               T1.WIND_HAIL_COUNT AS IN_COV_EXIST_COUNT,
               T1.MOLD_COUNT AS IN_MOLD_COV_COUNT,
               T1.BUILD_ORD_COUNT AS IN_BUILD_ORD_COV_COUNT,
               T1.LIMIT_AMT1 AS IN_FORMS_DET_LMT_AMT,
               T1.SCNDY_HME_FLG AS IN_SEC_OR_SEASONAL_IND,
               T1.STATE_CODE AS IN_POLICY_ST_CD,
               T1.UNIT_TYPE AS IN_DWELLING_FORM_CD,
               T1.lkp_LIMIT_AMT1 AS MOLD_II_SEC1_LIAB_LMT_AMT_TXT,
               T1.lkp_LIMIT_AMT2 AS MOLD_II_SEC2_LIAB_LMT_AMT_TXT,
               T1.source_record_id,
               T1.S10_ISO_PLOTA_EXTRACT_ID,
               T1.POL_PK,
               T1.ORIG_POL_PK,
               T1.lkp_DED_AMT1 AS IN_DED_AMT,
               T1.PPRC_COUNT AS IN_PPRC_COV_COUNT,
               T1.INC_LIMIT_C_COUNT AS IN_INC_LIMITC_COUNT,
               T1.PERS_PROP_COUNT AS IN_PERS_PROP_COUNT,
               T1.STATUS_COV_COUNT AS IN_STATUS_COV_COUNT,
               T1.RNTL_OTHR_COUNT AS IN_RNTL_OTHR_COV_COUNT,
               T1.BUILD_ADD1_COUNT AS IN_BUILD_ADD1_COV_COUNT,
               T1.FRAUD_COUNT AS IN_FRAUD_COV_COUNT,
               T1.lkp_PREMIUM_SOURCE_C AS IN_OIL_SPILL_COV,
               T1.lkp_PTCTV_DVC_BRGLR_ALM_TYP AS IN_BURGLAR_ALARM_TYPE_CD,
               T1.lkp_PTCTV_DVC_FIRE_ALM_TYP AS IN_FIRE_ALARM_TYPE_CD,
               T1.lkp_PTCTV_DVC_SPRNKLR_TYP AS IN_SPRINKLER_SYSTEM_TYPE_CD,
               T1.ISO_EXPOSURE AS IN_ISO_EXPOSURE,
               T1.LKP_MOLD_LIMITS_lkp_LIMIT_AMT1 AS MOLD_SEC1_LIAB_LMT_AMT_TXT,
               T1.LKP_MOLD_LIMITS_lkp_LIMIT_AMT2 AS MOLD_SEC2_LIAB_LMT_AMT_TXT,
               T1.STATE_CODE,
               T1.TERRTY_CD,
               T1.ISO_MODULE,
               T1.lkp_NAMD_STORM_DDCTBL_OPT AS NAMD_STORM_DDCTBL_OPT,
               T1.BUILD_ADDC_COUNT,
               T1.OUT_FRAUD_COUNT AS FRAUD_COUNT,
               T1.OIL_SPILL_COUNT,
               T1.BUS_PUR_C_COUNT,
               T1.COVA_SPCL_COUNT,
               T1.FARM_LIABC_COUNT,
               T1.INC_OCP_OC_COUNT,
               T1.INC_OCPNCY_COUNT,
               T1.LOSS_ASMTC_COUNT,
               T1.PI_COUNT,
               T1.RES_REN_C_COUNT,
               T1.RES_REN_TH_COUNT,
               T1.SINK_COV_COUNT,
               T1.SPCL_CMPTR_COUNT,
               T1.STRUC_RENT_C_COUNT,
               T1.THREE_FAM_COUNT,
               T1.WIND_HAIL_COUNT,
               T1.CUR_TERM_EFF_DT,
               T1.ISO_TERRITORY_CODE,
               T1.ISO_YEAR_OF_CONSTRUCTION_CODE AS IN_ISO_YEAR_OF_CONSTRUCTION_CODE,
               T1.ISO_ROOF_SURFACE_LOSS_CD,
               T1.ISO_ROOF_INSTALL_YR_CD,
               T1.ISO_RISK_IND_CD,
               T1.ISO_RISK_IND_CD_II,
               NULL AS BURGLAR_ALARM,
               NULL AS FIRE_ALARM,
               NULL AS IN_FORM_NBR_VALUE,
               lkp20.LIMIT_AMT1+0 AS v_COV_A_LIMIT_AMT,
               lkp21.LIMIT_AMT1+0 AS v_COV_B_LIMIT_AMT,
               lkp22.LIMIT_AMT1+0 AS v_COV_C_LIMIT_AMT,
               lkp23.LIMIT_AMT1+0 AS v_COV_D_LIMIT_AMT,
               lkp24.LIMIT_AMT1+0 AS v_COV_E_LIMIT_AMT,
               lkp25.LIMIT_AMT1+0 AS v_COV_F_LIMIT_AMT,
               lkp26.LIMIT_AMT1+0 AS v_BUILD_ORD_LIMIT_AMT,
               lkp26.LIMIT_AMT1+0 AS v_BUILD_ORD_LMT_AMT,
               IFF(RTRIM(LTRIM(IN_BURGLAR_ALARM_TYPE_CD)) IN('Reporting to Central Station','Central Station Police Alarm'),'Central Station', IFF(RTRIM(LTRIM(IN_BURGLAR_ALARM_TYPE_CD)) IN('Reporting to a Police Depart'),'Police Station', IFF(RTRIM(LTRIM(IN_BURGLAR_ALARM_TYPE_CD)) IN('Local Burglar Alarm'),'Local Station', 'No Burglar Alarm'))) AS v_BURGLAR_ALARMS,
               IFF(RTRIM(LTRIM(IN_FIRE_ALARM_TYPE_CD)) IN('Central Station Police/Fire','Reporting to Central Station'),'Central Station', IFF(RTRIM(LTRIM(IN_FIRE_ALARM_TYPE_CD)) IN('Reporting to Fire Department'),'Fire Station', IFF(RTRIM(LTRIM(IN_FIRE_ALARM_TYPE_CD)) IN('Fire or Smoke Detector','Fire/Smoke Detector'),'Local Station', 'No Fire Alarm'))) AS v_FIRE_ALARMS,
               IFF(RTRIM(LTRIM(IN_FIRE_ALARM_TYPE_CD))='Complete Sprinklers','All areas', IFF(RTRIM(LTRIM(IN_FIRE_ALARM_TYPE_CD))='Partial Sprinklers','All areas except', IFF(RTRIM(LTRIM(IN_SPRINKLER_SYSTEM_TYPE_CD))='In All Areas','All areas', IFF(RTRIM(LTRIM(IN_SPRINKLER_SYSTEM_TYPE_CD))='In Some areas','All areas except', 'No sprinkler installed')))) AS v_SPRINKLERS,
               TO_DECIMAL(NAMD_STORM_DDCTBL_OPT,2) AS v_WIND_DED_PCT,
               IFF(ISO_MODULE != 'HO',' ', IFF(WIND_HAIL_COUNT>=1 AND ROUND(v_WIND_DED_PCT * v_COV_A_LIMIT_AMT / 100) <= IN_DED_AMT,'5', IFF(WIND_HAIL_COUNT>=1 AND ROUND(v_WIND_DED_PCT * v_COV_A_LIMIT_AMT / 100) > IN_DED_AMT,'8', '5'))) AS v_OUT_TYPE_OF_DED_CD,      TO_VARCHAR('{{EFF_DT}}', 'YYYY-MM-DD')       AS V_EFF_DATE,
               TRUNC(V_EFF_DATE,'DD') AS TRUNC_EFF_DT,
               IFF( RTRIM(LTRIM(IN_DWELLING_FORM_CD)) IS NULL OR IN_COV_CD='FRAUD','  ', IFF(RTRIM(LTRIM(IN_DWELLING_FORM_CD)) IN( 'HO_4', 'HO_6'), '  ', IFF(STATE_CODE = 'MA' AND CUR_TERM_EFF_DT <TO_VARCHAR('2011-10-01', 'YYYY-MM-DD') and v_OUT_TYPE_OF_DED_CD = '8','02', IFF(STATE_CODE = 'CT' AND CUR_TERM_EFF_DT <TO_VARCHAR('2010-05-01', 'YYYY-MM-DD') and v_OUT_TYPE_OF_DED_CD = '8','02', IFF(STATE_CODE = 'NH' AND CUR_TERM_EFF_DT <TO_VARCHAR('2010-07-01', 'YYYY-MM-DD') and v_OUT_TYPE_OF_DED_CD = '8','02', IFF(v_OUT_TYPE_OF_DED_CD = '8' AND STATE_CODE = 'MA' AND ISO_TERRITORY_CODE IN('02','03','04','05','11','12','30','31','32','33','34','35','36','37','39','40','41','43'), DECODE(v_WIND_DED_PCT,1, '41',2, '42',3, '43',4, '44',5, '45',6, '89',7, '89',7.5, '46',10, '40','89'), IFF(v_OUT_TYPE_OF_DED_CD = '8' AND STATE_CODE = 'NH' AND ISO_TERRITORY_CODE = '01', DECODE(v_WIND_DED_PCT,1, '41',2, '42',3, '43',4, '44',5, '45',6, '89',7, '89',7.5, '46',10, '40','89'), IFF(v_OUT_TYPE_OF_DED_CD = '8' AND STATE_CODE = 'CT', DECODE(v_WIND_DED_PCT,1, '41',2, '42',3, '43',4, '44',5, '45',6, '89',7, '89',7.5, '46',10, '40','89'), '  ')))))))) AS v_OUT_WIND_DEDUCTIBLE_SIZE_CD,
               v_COV_A_LIMIT_AMT AS IN_COV_A_LIMIT_AMT,
               v_COV_B_LIMIT_AMT AS IN_COV_B_LIMIT_AMT,
               v_COV_C_LIMIT_AMT AS IN_COV_C_LIMIT_AMT,
               v_COV_D_LIMIT_AMT AS IN_COV_D_LIMIT_AMT,
               v_COV_E_LIMIT_AMT AS IN_COV_E_LIMIT_AMT,
               v_COV_F_LIMIT_AMT AS IN_COV_F_LIMIT_AMT,
               v_BUILD_ORD_LIMIT_AMT AS IN_BUILD_ORD_LIMIT_AMT,
               ROUND(v_COV_A_LIMIT_AMT*.02,0) AS IN_WIND_DED_AMT,
               v_BUILD_ORD_LMT_AMT AS IN_BUILD_ORD_LMT_AMT,
               '  ' AS OUT_HOME_BUS_CLASSIFICATION_CODE,
               '040' AS OUT_ANNUAL_STATEMENT_LINE_OF_BUSINESS_CODE,
               IFF( IN_DWELLING_FORM_CODE IS NULL,'    ', IFF((RTRIM(IN_DWELLING_FORM_CODE)='HO_3' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_4' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_5' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_6') AND (IN_PPRC_COV_COUNT<1 AND IN_INC_LIMITC_COUNT<1),'402', IFF((RTRIM(IN_DWELLING_FORM_CODE)='HO_3' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_5') AND (IN_PPRC_COV_COUNT>=1 AND IN_INC_LIMITC_COUNT<1),'403', IFF((RTRIM(IN_DWELLING_FORM_CODE)='HO_6' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_4')AND IN_PPRC_COV_COUNT>=1,'403', IFF((RTRIM(IN_DWELLING_FORM_CODE)='HO_6' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_4')AND (IN_PPRC_COV_COUNT<1 AND IN_INC_LIMITC_COUNT>=1),'403', IFF((RTRIM(IN_DWELLING_FORM_CODE)='HO_3' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_5') AND (IN_PPRC_COV_COUNT<1 AND IN_INC_LIMITC_COUNT>=1),'423', IFF((RTRIM(IN_DWELLING_FORM_CODE)='HO_3' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_5') AND (IN_PPRC_COV_COUNT>=1 AND IN_INC_LIMITC_COUNT>=1),'424', IFF(RTRIM(IN_DWELLING_FORM_CODE)='DP_3','428','   ')))))))) AS OUT_PERSONAL_LINES_STATESTICAL_PLAN_SUBLINE_CODE,
               IFF(RTRIM(IN_COV_CD) IN( 'WATER_BKUP','WATERBKUP2') and CUR_TERM_EFF_DT >=TO_VARCHAR('2011-10-01', 'YYYY-MM-DD'),'1',' ') AS OUT_EXCEPTION_CODE,
               IFF( IN_DWELLING_FORM_CODE IS NULL,' ',SUBSTR(IN_DWELLING_FORM_CODE,4,1)) AS OUT_POLICY_FORM_CODE,
               IFF( IN_DWELLING_FORM_CODE IS NULL,' ', IFF((RTRIM(IN_DWELLING_FORM_CODE)='HO_3' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_5') AND RTRIM(IN_TOWN_OR_ROW_HOUSE_IND)='N', IFF(RTRIM(IN_NBR_FAMILIES_QTY)>'2','6', DECODE(RTRIM(IN_NBR_FAMILIES_QTY),'1','1','2','3',' ')), IFF((RTRIM(IN_DWELLING_FORM_CODE)='HO_3' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_5') AND (RTRIM(IN_TOWN_OR_ROW_HOUSE_IND)='T' OR RTRIM(IN_TOWN_OR_ROW_HOUSE_IND)='R'),IFF(RTRIM(IN_NBR_FAMILIES_QTY)>'4','4',DECODE(RTRIM(IN_NBR_FAMILIES_QTY),'1','1','2','3','3','2','4','2',' ')), IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_4' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_6',IFF(RTRIM(IN_NBR_FAMILIES_QTY)>'4','8',DECODE(RTRIM(IN_NBR_FAMILIES_QTY),'1','1','2','2','3','3','4','4',' ')), IFF(RTRIM(IN_DWELLING_FORM_CODE)='DP_3' AND RTRIM(IN_NBR_FAMILIES_QTY)='1','1', IFF(RTRIM(IN_DWELLING_FORM_CODE)='DP_4' AND RTRIM(IN_NBR_FAMILIES_QTY)='2','3', IFF(RTRIM(IN_DWELLING_FORM_CODE)='DP_5' ,DECODE(RTRIM(IN_NBR_FAMILIES_QTY),'3','6','4','6',' '), IFF(RTRIM(IN_DWELLING_FORM_CODE)='DP_6' AND RTRIM(IN_NBR_FAMILIES_QTY)>'5','8',' ')))))))) AS OUT_NUMBER_OF_FAMILIES_CODE,
               IFF(RTRIM(IN_DWELLING_FORM_CODE) IN('HO_3','HO_4','HO_5','HO_6','DP_3') AND ( IN_BUILD_ORD_COV_COUNT IS NULL OR IN_BUILD_ORD_COV_COUNT = 0) ,'1', IFF(RTRIM(IN_DWELLING_FORM_CODE) IN('HO_3','HO_4','HO_5','HO_6','DP_3') AND IN_BUILD_ORD_COV_COUNT > 0 AND ( v_BUILD_ORD_LIMIT_AMT IS NULL OR v_BUILD_ORD_LIMIT_AMT= 0) ,'2', IFF(RTRIM(IN_DWELLING_FORM_CODE) IN('HO_3','HO_4','HO_5','HO_6','DP_3') AND IN_BUILD_ORD_COV_COUNT > 0 AND v_BUILD_ORD_LIMIT_AMT= 25,'3', IFF(RTRIM(IN_DWELLING_FORM_CODE) IN('HO_3','HO_4','HO_5','HO_6','DP_3') AND IN_BUILD_ORD_COV_COUNT > 0 AND v_BUILD_ORD_LIMIT_AMT= 50,'4', IFF(RTRIM(IN_DWELLING_FORM_CODE) IN('HO_3','HO_4','HO_5','HO_6','DP_3') AND IN_BUILD_ORD_COV_COUNT > 0 AND v_BUILD_ORD_LIMIT_AMT= 75,'5', IFF(RTRIM(IN_DWELLING_FORM_CODE) IN('HO_3','HO_4','HO_5','HO_6','DP_3') AND IN_BUILD_ORD_COV_COUNT > 0 AND v_BUILD_ORD_LIMIT_AMT= 100,'6', IFF(RTRIM(IN_DWELLING_FORM_CODE) IN('HO_3','HO_4','HO_5','HO_6','DP_3') AND IN_BUILD_ORD_COV_COUNT > 0 AND v_BUILD_ORD_LIMIT_AMT> 100,'9', ' '))))))) AS OUT_ORDINANCE_OR_LAW_COVERAGE_CODE,
               IFF(RTRIM(LTRIM(IN_COV_CD)) = 'FRAUD','E', IFF((RTRIM(IN_DWELLING_FORM_CODE) ='HO_3' OR RTRIM(IN_DWELLING_FORM_CODE) ='HO_4' OR RTRIM(IN_DWELLING_FORM_CODE) ='HO_5') AND IN_PERS_PROP_COUNT<1 AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='P', DECODE(TRUE, IN_STATUS_COV_COUNT<1 AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N','1', IN_STATUS_COV_COUNT>=1 AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N','2', '1'), IFF((RTRIM(IN_DWELLING_FORM_CODE) ='HO_3' OR RTRIM(IN_DWELLING_FORM_CODE) ='HO_4' OR RTRIM(IN_DWELLING_FORM_CODE) ='HO_5') AND IN_PERS_PROP_COUNT<1 AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)<>'P', DECODE(TRUE, IN_STATUS_COV_COUNT<1 AND RTRIM(IN_SEC_OR_SEASONAL_IND)<>'E','3', IN_STATUS_COV_COUNT>=1 AND RTRIM(IN_SEC_OR_SEASONAL_IND)='E','4', '1'), IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_3' AND IN_PERS_PROP_COUNT>=1,'7', IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_6' AND IN_BUILD_ADD1_COV_COUNT<1, DECODE(TRUE, IN_STATUS_COV_COUNT<1 AND IN_RNTL_OTHR_COV_COUNT<1 AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N' AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='P','1', IN_STATUS_COV_COUNT>=1 AND IN_RNTL_OTHR_COV_COUNT<1 AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N' AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='P','2', IN_STATUS_COV_COUNT<1 AND IN_RNTL_OTHR_COV_COUNT>=1 AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='R','5', IN_STATUS_COV_COUNT>=1 AND IN_RNTL_OTHR_COV_COUNT>=1 AND RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='R','6', RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='S' AND RTRIM(IN_SEC_OR_SEASONAL_IND)<>'E','3', RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='S' AND RTRIM(IN_SEC_OR_SEASONAL_IND)='E','4', '1'), IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_6' AND IN_BUILD_ADD1_COV_COUNT>=1,'7', IFF(RTRIM(IN_DWELLING_FORM_CODE)='DP_3', DECODE(TRUE, RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='P' AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N','1', RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)='P' AND RTRIM(IN_SEC_OR_SEASONAL_IND)<>'N','3', RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)<>'P' AND RTRIM(IN_SEC_OR_SEASONAL_IND)='N','5', RTRIM(IN_DF_DWELLING_USE_OCCUP_CD)<>'P' AND RTRIM(IN_SEC_OR_SEASONAL_IND)<>'N','7', '1')))))))) AS OUT_STATUS_CODE,
               IFF(IN_MOLD_COV_COUNT>=1,DECODE(TRUE,MOLD_SEC1_LIAB_LMT_AMT_TXT=10000 AND MOLD_SEC2_LIAB_LMT_AMT_TXT=50000,'1',MOLD_SEC1_LIAB_LMT_AMT_TXT=10000 AND MOLD_SEC2_LIAB_LMT_AMT_TXT=100000,'2',MOLD_SEC1_LIAB_LMT_AMT_TXT=25000 AND MOLD_SEC2_LIAB_LMT_AMT_TXT=50000,'3',MOLD_SEC1_LIAB_LMT_AMT_TXT=25000AND MOLD_SEC2_LIAB_LMT_AMT_TXT=100000,'4',MOLD_SEC1_LIAB_LMT_AMT_TXT=50000 AND MOLD_SEC2_LIAB_LMT_AMT_TXT=50000,'5',MOLD_SEC1_LIAB_LMT_AMT_TXT=50000 AND MOLD_SEC2_LIAB_LMT_AMT_TXT=100000,'6','7'),'9') AS OUT_MOLD_COVERAGE_CODE,
               ' ' AS OUT_THEFT_DED_SIZE_CD,
               v_OUT_TYPE_OF_DED_CD AS OUT_TYPE_OF_DED_CD,
               IFF( IN_DED_AMT IS NULL or IN_DED_AMT=0,'01', IFF(IN_DED_AMT>10000,'96', IFF(IN_DED_AMT>=1 and IN_DED_AMT<=49,'98', DECODE(IN_DED_AMT,25,'99',50,'05',100,'10',200,'20',250,'25', 300,'99',350,'99',400,'40',450,'99',500,'50',600,'60',750,'75',800,'80',1000,'82',1200,'84',1500,'85',2000,'87',2500,'88',3000,'89',4000,'90',5000,'91',7500,'92',10000,'93', IFF(IN_DED_AMT>50 and IN_DED_AMT<10000,'99','  '))))) AS OUT_FIRE_DED_SIZE_CD,
               DECODE(TRUE, CUR_TERM_EFF_DT >=TRUNC_EFF_DT, IFF(RTRIM(IN_PLCY_ST_CD)='MA', IFF(LTRIM(RTRIM(IN_OIL_SPILL_COV))= 'OIL_SPILL', IFF( IN_DWELLING_FORM_CODE IS NULL,'9', DECODE(RTRIM(IN_DWELLING_FORM_CODE), 'DP_3','B', 'HO_3','B', 'HO_5','B', 'HO_6','B', '9')),'9'),'9'), CUR_TERM_EFF_DT <TRUNC_EFF_DT, IFF(RTRIM(IN_PLCY_ST_CD)='MA', IFF(LTRIM(RTRIM(IN_OIL_SPILL_COV))= 'OIL_SPILL', IFF( IN_DWELLING_FORM_CODE IS NULL,'9', DECODE(RTRIM(IN_DWELLING_FORM_CODE), 'DP_3','1', 'HO_3','1', 'HO_5','1', 'HO_6','1', '9')),'9'),'9') ,'9') AS OUT_ENV_IMP_COV_CD1,
               DECODE(TRUE, CUR_TERM_EFF_DT >=TRUNC_EFF_DT, IFF(RTRIM(IN_PLCY_ST_CD)='MA', IFF(LTRIM(RTRIM(IN_OIL_SPILL_COV))='OIL_SPILL', IFF( IN_DWELLING_FORM_CODE IS NULL,'9', DECODE(RTRIM(IN_DWELLING_FORM_CODE), 'HO_3','B', 'HO_5','B', 'HO_6','B', '9')),'9'),'9') , CUR_TERM_EFF_DT <TRUNC_EFF_DT, IFF(RTRIM(IN_PLCY_ST_CD)='MA', IFF(LTRIM(RTRIM(IN_OIL_SPILL_COV))='OIL_SPILL', IFF( IN_DWELLING_FORM_CODE IS NULL,'9', DECODE(RTRIM(IN_DWELLING_FORM_CODE), 'HO_3','1', 'HO_5','1', 'HO_6','1', '9')),'9'),'9') ,'9') AS OUT_ENV_IMP_COV_CD2,
               IFF( IN_DWELLING_FORM_CODE IS NULL,' ',IFF(RTRIM(IN_DWELLING_FORM_CODE)<>'HO_6',' ', IFF(v_COV_A_LIMIT_AMT >1000 AND v_COV_A_LIMIT_AMT<=9999,'2', IFF(v_COV_A_LIMIT_AMT >=10000 AND v_COV_A_LIMIT_AMT<=19999,'3', IFF(v_COV_A_LIMIT_AMT >=20000 AND v_COV_A_LIMIT_AMT<=29999,'4', IFF(v_COV_A_LIMIT_AMT >=30000 AND v_COV_A_LIMIT_AMT<=39999,'5', IFF(v_COV_A_LIMIT_AMT >=40000 AND v_COV_A_LIMIT_AMT<=49999,'6', IFF(v_COV_A_LIMIT_AMT >=50000 AND v_COV_A_LIMIT_AMT<=59999,'7', IFF(v_COV_A_LIMIT_AMT >=60000 AND v_COV_A_LIMIT_AMT<=69999,'8', IFF(v_COV_A_LIMIT_AMT >=70000,'9', '1')))))))))) AS OUT_LIMIT_OF_LIAB_CD,
               IFF( IN_DWELLING_FORM_CODE IS NULL,' ', DECODE(RTRIM(IN_DWELLING_FORM_CODE),'HO_3','1','HO_4','1','HO_6','1','HO_5','9','DP_3','3','9')) AS OUT_POLICY_PRGRM_CD,
               IFF( IN_DWELLING_FORM_CODE IS NULL,' ',IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_5',' ', IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_3' AND v_COV_A_LIMIT_AMT<>0 ,IFF((v_COV_C_LIMIT_AMT/v_COV_A_LIMIT_AMT)>0.4, DECODE((v_COV_C_LIMIT_AMT/v_COV_A_LIMIT_AMT),0.4,'2',0.5,'3', 0.6,'4',0.7,'4',0.75,'6',1,'7','9'),'1')))) AS OUT_COV_C_PCT_CD,
               DECODE(TRUE,v_COV_E_LIMIT_AMT=25000,'1', v_COV_E_LIMIT_AMT=50000,'2', v_COV_E_LIMIT_AMT=100000,'3', v_COV_E_LIMIT_AMT=200000,'4', v_COV_E_LIMIT_AMT=300000,'5', v_COV_E_LIMIT_AMT=400000,'6', v_COV_E_LIMIT_AMT=500000,'7', v_COV_E_LIMIT_AMT>500000,'8', v_COV_E_LIMIT_AMT<25000,'9', '9') AS OUT_COV_E_LIAB_CD,
               IFF( IN_DWELLING_FORM_CODE IS NULL,'    ', IFF(IN_COV_CD = 'WATER_BKUP', '0005', IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_4', LPAD(TO_CHAR(ROUND((v_COV_C_LIMIT_AMT/1000))),4,'0'), IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_6',LPAD(TO_CHAR(ROUND(((v_COV_A_LIMIT_AMT+v_COV_C_LIMIT_AMT)/1000))),4,'0'), IFF(RTRIM(IN_DWELLING_FORM_CODE)='HO_3' OR RTRIM(IN_DWELLING_FORM_CODE)='HO_5' OR RTRIM(IN_DWELLING_FORM_CODE)='DP_3', LPAD(TO_CHAR(ROUND((v_COV_A_LIMIT_AMT/1000))),4,'0'), IN_ISO_EXPOSURE))))) AS OUT_EXPOSURE,
               ' ' AS OUT_MOB_HOM_END_ID,
               ' ' AS OUT_TIE_DOWN,
               IFF( IN_POLICY_ST_CD IS NULL OR IN_POLICY_ST_CD='NH',' ', IFF(IN_POLICY_ST_CD = 'CT','1', IFF(IN_CONSTRUCTION_YEAR>'1978','9','5'))) AS OUT_STATE_EXCEPTION_INDICATOR_CODE_I,
               ' ' AS OUT_STATE_EXCP_IND_CD2,
               ' ' AS OUT_STATE_EXCP_IND_CD3,
               IFF(IN_BUILD_ORD_COV_COUNT<1,'2',DECODE(TRUE, IN_FORMS_DET_LMT_AMT=25 OR v_BUILD_ORD_LMT_AMT=25,'3', IN_FORMS_DET_LMT_AMT=50 OR v_BUILD_ORD_LMT_AMT=50,'4', IN_FORMS_DET_LMT_AMT=75 OR v_BUILD_ORD_LMT_AMT=75,'5', IN_FORMS_DET_LMT_AMT=100 OR v_BUILD_ORD_LMT_AMT=100,'6', IN_FORMS_DET_LMT_AMT>100 OR v_BUILD_ORD_LMT_AMT>100,'9','2')) AS OUT_ORD_OR_LAW_CD,
               v_OUT_WIND_DEDUCTIBLE_SIZE_CD AS OUT_WIND_DEDUCTIBLE_SIZE_CD,
               '1' AS OUT_WINDSTRM_HAIL_COV_CD,
               '10' AS OUT_COMP_EXCEP_IND,
               '9' AS OUT_STAT_PLAN_IND_CD,
               IFF(RTRIM(LTRIM(IN_DWELLING_FORM_CODE)) IN( 'HO_4', 'HO_6'), '  ',IN_ISO_YEAR_OF_CONSTRUCTION_CODE) AS OUT_YEAR_OF_CONSTRUCTION_CODE,
               DECODE(TRUE, RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '01', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '02', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '05', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '06', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '07', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '10', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '11', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '12', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '15', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '16', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '17', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '20', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '21', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '22', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '25', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '26', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '27', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '30', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '31', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '32', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '35', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '36', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '3', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Police Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '40', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '41', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '42', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '45', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '46', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '47', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '50', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '51', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '52', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '55', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '56', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '57', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '60', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '61', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '62', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Central Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '65', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '66', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '67', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Fire Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '70', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '71', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '72', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'Local Station' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '75', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas', '76', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'All areas except', '77', RTRIM(LTRIM(v_BURGLAR_ALARMS)) = 'No Burglar Alarm' and RTRIM(LTRIM(v_FIRE_ALARMS)) = 'No Fire Alarm' and RTRIM(LTRIM(v_SPRINKLERS)) = 'No sprinkler installed', '80', '80') AS OUT_PROTECTIVE_DEVICE_CODE,
               'BAFA' AS RECORD_TYPE_CD
          FROM Exp_Inputs_before_agg_DW as T1
         INNER JOIN EXP_COVERAGE_EXISTS_COUNT
            ON Exp_Inputs_before_agg_DW.source_record_id = EXP_COVERAGE_EXISTS_COUNT.source_record_id -- writting query for unconnected lookup function

          LEFT JOIN (
                select , h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       h.LIMIT_AMT1 as LIMIT_AMT1 ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.LIMIT_AMT1,
                          h.PREMIUM_SOURCE_C
               ) lkp20
            on lkp20.POL_PK=T1.in_POL_PK
           AND lkp20.ORIG_POL_PK=T1.in_ORIG_POL_PK
           AND lkp20.PREMIUM_SOURCE_C=T1.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select , h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       h.LIMIT_AMT1 as LIMIT_AMT1 ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.LIMIT_AMT1,
                          h.PREMIUM_SOURCE_C
               ) lkp21
            on lkp21.POL_PK=T1.in_POL_PK
           AND lkp21.ORIG_POL_PK=T1.in_ORIG_POL_PK
           AND lkp21.PREMIUM_SOURCE_C=T1.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select , h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       h.LIMIT_AMT1 as LIMIT_AMT1 ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.LIMIT_AMT1,
                          h.PREMIUM_SOURCE_C
               ) lkp22
            on lkp22.POL_PK=T1.in_POL_PK
           AND lkp22.ORIG_POL_PK=T1.in_ORIG_POL_PK
           AND lkp22.PREMIUM_SOURCE_C=T1.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select , h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       h.LIMIT_AMT1 as LIMIT_AMT1 ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.LIMIT_AMT1,
                          h.PREMIUM_SOURCE_C
               ) lkp23
            on lkp23.POL_PK=T1.in_POL_PK
           AND lkp23.ORIG_POL_PK=T1.in_ORIG_POL_PK
           AND lkp23.PREMIUM_SOURCE_C=T1.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select , h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       h.LIMIT_AMT1 as LIMIT_AMT1 ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.LIMIT_AMT1,
                          h.PREMIUM_SOURCE_C
               ) lkp24
            on lkp24.POL_PK=T1.in_POL_PK
           AND lkp24.ORIG_POL_PK=T1.in_ORIG_POL_PK
           AND lkp24.PREMIUM_SOURCE_C=T1.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select , h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       h.LIMIT_AMT1 as LIMIT_AMT1 ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.LIMIT_AMT1,
                          h.PREMIUM_SOURCE_C
               ) lkp25
            on lkp25.POL_PK=T1.in_POL_PK
           AND lkp25.ORIG_POL_PK=T1.in_ORIG_POL_PK
           AND lkp25.PREMIUM_SOURCE_C=T1.in_PREMIUM_SOURCE_C
          LEFT JOIN (
                select , h.POL_PK as POL_PK ,
                       h.ORIG_POL_PK as ORIG_POL_PK ,
                       h.LIMIT_AMT1 as LIMIT_AMT1 ,
                       LTRIM(RTRIM(h.PREMIUM_SOURCE_C)) as PREMIUM_SOURCE_C,
                       row_number() over(partition by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C order by POL_PK, ORIG_POL_PK, PREMIUM_SOURCE_C) as rn -- Please review the order by columns

                  from BURRPT.S10_ISO_PLOTA_EXTRACT_NE h qualify rn = 1
                 GROUP BY h.POL_PK,
                          h.ORIG_POL_PK,
                          h.LIMIT_AMT1,
                          h.PREMIUM_SOURCE_C
               ) lkp26
            on lkp26.POL_PK=T1.in_POL_PK
           AND lkp26.ORIG_POL_PK=T1.in_ORIG_POL_PK
           AND lkp26.PREMIUM_SOURCE_C=T1.in_PREMIUM_SOURCE_C
       ) 
       
       select * from EXP_SUBMISSION_ID limit 5
      
