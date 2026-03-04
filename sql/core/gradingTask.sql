/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: gradingTask.csv
============================================================================ */

set heading off
set pagesize 0
set feedback off
set underline off
set trimspool on
set linesize 32767

WITH ps_source AS (
  SELECT DISTINCT
    SUBSTR(TRIM(storecode),1,25) AS storeCode
  FROM ps.storedgrades
  WHERE storecode IS NOT NULL
    AND LENGTH(REGEXP_REPLACE(storecode, '[[:space:]]', '')) > 0
),

xform AS (
  SELECT
    CASE
      WHEN REGEXP_LIKE(UPPER(storeCode), '^Q[1-9]$') THEN 'Quarter ' || SUBSTR(storeCode,2,1) || ' Grade'
      WHEN REGEXP_LIKE(UPPER(storeCode), '^S[1-9]$') THEN 'Semester ' || SUBSTR(storeCode,2,1) || ' Grade'
      WHEN UPPER(storeCode) IN ('Y','Y1','Y2') THEN 'Final Grade'
      WHEN UPPER(storeCode) IN ('F','F1','FE') THEN 'Final Exam'
      WHEN UPPER(storeCode) IN ('P','PR') THEN 'Progress'
      ELSE storeCode
    END AS taskName,

    SUBSTR(storeCode,1,25) AS taskCode,

    CASE
      WHEN REGEXP_LIKE(UPPER(storeCode), '^Q[1-9]$') THEN TO_NUMBER(SUBSTR(storeCode,2,1))
      WHEN REGEXP_LIKE(UPPER(storeCode), '^S[1-9]$') THEN 10 + TO_NUMBER(SUBSTR(storeCode,2,1))
      WHEN UPPER(storeCode) IN ('Y','Y1','Y2') THEN 99
      ELSE 500
    END AS taskSeq,

    CASE WHEN UPPER(storeCode) IN ('Y','Y1','Y2','S1','S2') THEN 1 ELSE 0 END AS postToTranscript,

    storeCode AS sourceStoreCode
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE
        WHEN taskName IS NULL OR TRIM(taskName) = ''
        THEN 'ERR_REQUIRED_taskName; '
      END
    ) AS validation_errors
  FROM xform x
),

final_data AS (
  SELECT
    CASE
      WHEN validation_errors IS NULL OR validation_errors = ''
      THEN 'CLEAN'
      ELSE 'ERROR'
    END AS row_type,

    '"' || REPLACE(REPLACE(REPLACE(SUBSTR(TRIM(taskName),1,75),'"','""'),CHR(13),' '),CHR(10),' ') || '"' AS taskName,
    '"' || REPLACE(REPLACE(REPLACE(SUBSTR(TRIM(taskCode),1,25),'"','""'),CHR(13),' '),CHR(10),' ') || '"' AS taskCode,

    taskSeq,
    postToTranscript,

    '"' || REPLACE(REPLACE(REPLACE(sourceStoreCode,'"','""'),CHR(13),' '),CHR(10),' ') || '"' AS sourceStoreCode,

    '"' || REPLACE(REPLACE(REPLACE(NVL(validation_errors,''),'"','""'),CHR(13),' '),CHR(10),' ') || '"' AS validation_errors
  FROM validated

  UNION ALL

  SELECT
    'ERROR',
    '"MISSING_SOURCE_TASKS"',
    NULL,
    999,
    0,
    NULL,
    '"ERR_NO_STORECODES_IN_PS.STOREDGRADES; "'
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM validated)
),

data_rows AS (
  SELECT ROW_NUMBER() OVER (ORDER BY taskSeq, taskName) AS data_order, f.*
  FROM final_data f
)

SELECT line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
         'row_type,taskName,taskCode,taskSeq,postToTranscript,sourceStoreCode,validation_errors' AS line
  FROM dual

  UNION ALL

  SELECT
    1,
    data_order,
    NVL(row_type,'') || ',' ||
    NVL(taskName,'') || ',' ||
    NVL(taskCode,'') || ',' ||
    NVL(TO_CHAR(taskSeq),'') || ',' ||
    NVL(TO_CHAR(postToTranscript),'') || ',' ||
    NVL(sourceStoreCode,'') || ',' ||
    NVL(validation_errors,'')
  FROM data_rows
)
ORDER BY sort_order, data_order;