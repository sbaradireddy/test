
EXP_NJ_DMV_MTHLY_MERGE_HEADER_FF AS (
    -- Ensure HEADER is generated in this CTE
    SELECT 
        source_record_id,
        OUT_DMV_INS_COMPANY_CD AS DMV_INS_COMPANY_CD,
        DMV_TIME_STAMP,
        DMV_DATE_STAMP1 AS DMV_DATE_STAMP,
        BATCH_END_DT,
        RPAD('HEADER', 6, ' ') AS DMV_RECORD_NAME,
        'N' AS DMV_FILE_TYPE,
        TO_CHAR(DATEADD('D', 7, LAST_DAY(BATCH_END_DT)), 'MMDDYYYY') AS DMV_ORIG_CYCLE_DUE_DATE_V,
        RPAD(' ', 167, ' ') AS FILLER,
        -- Construct HEADER field
        DMV_RECORD_NAME || DMV_FILE_TYPE || DMV_INS_COMPANY_CD || DMV_TIME_STAMP || DMV_DATE_STAMP || DMV_ORIG_CYCLE_DUE_DATE_V || FILLER AS HEADER
    FROM EXP_NJ_DMV_MTHLY_MERGE_DATA
),

AGG_NJ_DMV_MTHLY_MERGE_HEADER_FF AS (
    -- Aggregate HEADER records as needed
    SELECT 
        source_record_id,
        HEADER,  -- Select HEADER here for further use
        1 AS SORT_ORDER
    FROM EXP_NJ_DMV_MTHLY_MERGE_HEADER_FF
    GROUP BY HEADER, source_record_id
)
