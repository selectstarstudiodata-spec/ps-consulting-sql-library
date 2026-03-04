set heading off
set feedback off
set pagesize 0
set verify off
set trimspool on
set underline off
set newpage none
set linesize 32767

/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: behaviorResolution.csv
============================================================================ */

WITH params AS (
  SELECT
    DATE '2019-07-01' AS win_start,
    DATE '2027-06-30' AS win_end
  FROM dual
),

has_rows AS (
  SELECT
    CASE
      WHEN EXISTS (SELECT 1 FROM ps.incident_action WHERE ROWNUM = 1)
      THEN 1 ELSE 0
    END AS has_incident_action
  FROM dual
),

ps_source AS (
  SELECT
    CAST(ia.incident_id        AS VARCHAR2(50))  AS incidentID_raw,
    CAST(ia.incident_action_id AS VARCHAR2(50))  AS resolutionID_raw,
    CAST(
      COALESCE(
        NULLIF(TRIM(ia.action_resolved_desc), ''),
        'INCIDENT_ACTION'
      ) AS VARCHAR2(100)
    ) AS resolutionName_raw,
    ia.action_plan_begin_dt      AS beginDate_raw,
    ia.action_plan_end_dt        AS endDate_raw,
    ia.action_actual_resolved_dt AS resolvedDate_raw
  FROM ps.incident_action ia
  WHERE (SELECT has_incident_action FROM has_rows) = 1
),

xform AS (
  SELECT
    SUBSTR(TRIM(incidentID_raw), 1, 50)      AS incidentID,
    SUBSTR(TRIM(resolutionID_raw), 1, 50)    AS resolutionID,
    SUBSTR(TRIM(resolutionName_raw), 1, 100) AS resolutionName,
    beginDate_raw    AS beginDate,
    endDate_raw      AS endDate,
    resolvedDate_raw AS resolvedDate
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN incidentID IS NULL OR TRIM(incidentID) = '' THEN 'ERR_REQUIRED_incidentID; ' END ||
      CASE WHEN resolutionID IS NULL OR TRIM(resolutionID) = '' THEN 'ERR_REQUIRED_resolutionID; ' END ||
      CASE WHEN resolutionName IS NULL OR TRIM(resolutionName) = '' THEN 'ERR_REQUIRED_resolutionName; ' END ||
      CASE
        WHEN COALESCE(beginDate, endDate, resolvedDate) IS NOT NULL
         AND (TRUNC(COALESCE(beginDate, endDate, resolvedDate)) < (SELECT win_start FROM params)
           OR TRUNC(COALESCE(beginDate, endDate, resolvedDate)) > (SELECT win_end FROM params))
        THEN 'ERR_OUTSIDE_7YR_WINDOW; '
      END
    ) AS validation_errors
  FROM xform x
),

real_rows AS (
  SELECT
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END AS row_type,
    incidentID,
    resolutionID,
    resolutionName,
    beginDate,
    endDate,
    resolvedDate,
    validation_errors
  FROM validated
),

info_row AS (
  SELECT
    'INFO' AS row_type,
    CAST(NULL AS VARCHAR2(50))  AS incidentID,
    CAST(NULL AS VARCHAR2(50))  AS resolutionID,
    CAST(NULL AS VARCHAR2(100)) AS resolutionName,
    CAST(NULL AS DATE)          AS beginDate,
    CAST(NULL AS DATE)          AS endDate,
    CAST(NULL AS DATE)          AS resolvedDate,
    'NO_SOURCE_ROWS_IN_PS.INCIDENT_ACTION' AS validation_errors
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
  '"row_type","incidentID","resolutionID","resolutionName","beginDate","endDate","resolvedDate","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    1,
    ROW_NUMBER() OVER (ORDER BY row_type DESC, incidentID, resolutionID),

    '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
    '"'||REPLACE(NVL(incidentID,''),'"','""')||'",'||
    '"'||REPLACE(NVL(resolutionID,''),'"','""')||'",'||
    '"'||REPLACE(NVL(resolutionName,''),'"','""')||'",'||
    '"'||NVL(TO_CHAR(beginDate,'YYYY-MM-DD'),'')||'",'||
    '"'||NVL(TO_CHAR(endDate,'YYYY-MM-DD'),'')||'",'||
    '"'||NVL(TO_CHAR(resolvedDate,'YYYY-MM-DD'),'')||'",'||
    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'

  FROM final_output
)
ORDER BY sort_order, data_order;