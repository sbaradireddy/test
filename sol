EXP_SUBMISSION_ID AS (
    -- Writing query for expression function
    SELECT 
        source_record_id,
        ISO_BURRPT_QTR_SUBM_ID AS IN_ISO_BURRPT_QTR_SUBM_ID,
        IN_RPRTG_PERIOD_END_DT AS IN_RPRTG_END_PRD,
        IFF(
            ISO_BURRPT_QTR_SUBM_ID IS NULL,
            1,
            CAST(SUBSTR(ISO_BURRPT_QTR_SUBM_ID, 1, REGEXP_INSTR(ISO_BURRPT_QTR_SUBM_ID, '-') - 1) AS INT) + 1
        ) AS v_ISO_BURRPT_QTR_SUBM_ID,
        TO_CHAR(IN_RPRTG_PERIOD_END_DT, 'MMYYYY') AS v_RPRTG_PERIOD_END_DT,
        CONCAT(
            IFF(
                ISO_BURRPT_QTR_SUBM_ID IS NULL,
                1,
                CAST(SUBSTR(ISO_BURRPT_QTR_SUBM_ID, 1, REGEXP_INSTR(ISO_BURRPT_QTR_SUBM_ID, '-') - 1) AS INT) + 1
            ),
            '-',
            TO_CHAR(IN_RPRTG_PERIOD_END_DT, 'MMYYYY')
        ) AS OUT_ISO_BURRPT_QTR_SUBM_ID
    FROM Exp_Inputs_before_agg_DW
)
