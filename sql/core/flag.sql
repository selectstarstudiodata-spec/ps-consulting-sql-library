/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: flag.csv

  Required Columns: flagName
  Unique Key: flagName

  Business Rules:
  - 1 active.
  - Never fabricate flags.
  - Always emit at least one row (ERROR) when no source rows are found so district DQ review is never "silent empty".

  Oracle SQL
============================================================================ */
set heading off
set pagesize 0
set feedback off
set underline off

WITH params AS (
  SELECT 2027 AS active_end_year FROM dual
),
yr AS (
  SELECT (active_end_year - 1990) AS requested_yearid FROM params
),
active_yearid AS (
  SELECT
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM ps.cc cc
        WHERE cc.termid IS NOT NULL
          AND FLOOR(cc.termid/100) = (SELECT requested_yearid FROM yr)
      )
      THEN (SELECT requested_yearid FROM yr)
      ELSE (SELECT MAX(FLOOR(termid/100)) FROM ps.cc WHERE termid IS NOT NULL)
    END AS yearid
  FROM dual
),

ps_source AS (
  SELECT CAST(NULL AS VARCHAR2(50)) AS flagName
  FROM dual
  WHERE 1=0
),

xform AS (
  SELECT DISTINCT
    SUBSTR(TRIM(flagName),1,50) AS flagName
  FROM ps_source
  WHERE flagName IS NOT NULL
    AND TRIM(flagName) <> ''
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN flagName IS NULL OR TRIM(flagName) = '' THEN 'ERR_REQUIRED_flagName; ' END
    ) AS validation_errors
  FROM xform x
),

final_rows AS (
  SELECT
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END AS row_type,
    flagName,
    validation_errors
  FROM validated

  UNION ALL

  SELECT
    'ERROR' AS row_type,
    'MISSING_SOURCE_FLAGS' AS flagName,
    'ERR_NO_FLAG_SOURCE_CONFIGURED_OR_NO_FLAG_ROWS; ' AS validation_errors
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM validated)
)

SELECT line
FROM (
    SELECT 0 AS sort_order,
           '"row_type","flagName","validation_errors"' AS line
    FROM dual

    UNION ALL

    SELECT 1,
           '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
           '"'||REPLACE(NVL(flagName,''),'"','""')||'",'||
           '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
    FROM final_rows
)
ORDER BY sort_order;