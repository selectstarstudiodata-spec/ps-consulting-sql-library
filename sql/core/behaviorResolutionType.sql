set heading off
set pagesize 0
set feedback off
set underline off
set trimspool on
set linesize 32767
set newpage none

/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: behaviorResolutionType.csv
============================================================================ */

WITH src_counts AS (
  SELECT COUNT(*) AS cnt_incident_action
  FROM ps.incident_action
),

has_rows AS (
  SELECT CASE WHEN cnt_incident_action > 0 THEN 1 ELSE 0 END AS has_incident_action
  FROM src_counts
),

ps_source AS (
  SELECT DISTINCT
    SUBSTR(
      TRIM(
        COALESCE(
          NULLIF(TRIM(ia.action_resolved_desc), ''),
          'INCIDENT_ACTION'
        )
      ),
      1,
      100
    ) AS resolutionName
  FROM ps.incident_action ia
  WHERE (SELECT has_incident_action FROM has_rows) = 1
),

validated AS (
  SELECT
    resolutionName,
    TRIM(
      CASE
        WHEN resolutionName IS NULL OR TRIM(resolutionName) = ''
        THEN 'ERR_REQUIRED_resolutionName; '
      END
    ) AS validation_errors
  FROM ps_source
),

real_rows AS (
  SELECT
    CASE
      WHEN validation_errors IS NULL OR validation_errors = ''
      THEN 'CLEAN'
      ELSE 'ERROR'
    END AS row_type,
    resolutionName,
    validation_errors
  FROM validated
),

info_row AS (
  SELECT
    'INFO' AS row_type,
    CAST(NULL AS VARCHAR2(100)) AS resolutionName,
    'NO_SOURCE_ROWS_IN_PS.INCIDENT_ACTION (COUNT=' ||
    (SELECT cnt_incident_action FROM src_counts) || ')' AS validation_errors
  FROM dual
  WHERE (SELECT has_incident_action FROM has_rows) = 0
),

final_output AS (
  SELECT * FROM real_rows
  UNION ALL
  SELECT * FROM info_row
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
         '"row_type","resolutionName","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    1,
    ROW_NUMBER() OVER (ORDER BY row_type DESC, resolutionName),

    '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
    '"'||REPLACE(NVL(resolutionName,''),'"','""')||'",'||
    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'

  FROM final_output
)
ORDER BY sort_order, data_order;