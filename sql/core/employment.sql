/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: employment.csv

  Required Columns: employNum, schoolNum, startDate
  Unique Key: employNum, schoolNum, startDate, assignmentCode

  Business Rules:
  - Years-of-data rule: 1 active.
  - Always emit at least one row (ERROR) when no source rows are found so district DQ review is never "silent empty".
  - Derive employment assignments from PS.SCHOOLSTAFF for the active year window (PS.TERMS).
  - No district edits required (plug-and-play).

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
        FROM ps.terms t
        WHERE t.isyearrec = 1
          AND t.yearid = (SELECT requested_yearid FROM yr)
      )
      THEN (SELECT requested_yearid FROM yr)
      ELSE (SELECT MAX(t.yearid) FROM ps.terms t WHERE t.isyearrec = 1)
    END AS yearid
  FROM dual
),
active_year_window AS (
  SELECT
    MIN(t.firstday) AS year_start,
    MAX(t.lastday)  AS year_end
  FROM ps.terms t
  JOIN active_yearid ay
    ON ay.yearid = t.yearid
  WHERE t.isyearrec = 1
),

ps_source AS (
  SELECT
    SUBSTR(
      COALESCE(
        NULLIF(TRIM(u.teachernumber), ''),
        TO_CHAR(u.dcid)
      ),
      1, 15
    ) AS employNum,

    SUBSTR(TRIM(sc.school_number),1,7) AS schoolNum,

    (SELECT year_start FROM active_year_window) AS startDate,

    'PRIMARY' AS assignmentCode

  FROM ps.schoolstaff ss
  JOIN ps.users u
    ON u.dcid = ss.users_dcid
  JOIN ps.schools sc
    ON sc.school_number = ss.schoolid
  JOIN active_year_window yw
    ON 1=1
  WHERE NVL(u.ptaccess,0) = 1
    AND NVL(ss.status,1) = 1
    AND NVL(ss.staffstatus,1) = 1
),

xform AS (
  SELECT
    employNum,
    schoolNum,
    startDate,
    assignmentCode
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN employNum IS NULL OR TRIM(employNum) = '' THEN 'ERR_REQUIRED_employNum; ' END ||
      CASE WHEN schoolNum IS NULL OR TRIM(schoolNum) = '' THEN 'ERR_REQUIRED_schoolNum; ' END ||
      CASE WHEN startDate IS NULL THEN 'ERR_REQUIRED_startDate; ' END ||
      CASE WHEN assignmentCode IS NULL OR TRIM(assignmentCode) = '' THEN 'ERR_REQUIRED_assignmentCode; ' END
    ) AS validation_errors
  FROM xform x
),

final_rows AS (
  SELECT
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END AS row_type,
    employNum,
    schoolNum,
    startDate,
    assignmentCode,
    validation_errors
  FROM validated

  UNION ALL

  SELECT
    'ERROR' AS row_type,
    NULL AS employNum,
    NULL AS schoolNum,
    (SELECT year_start FROM active_year_window) AS startDate,
    'PRIMARY' AS assignmentCode,
    'ERR_NO_SOURCE_ROWS_FOR_EMPLOYMENT; ' AS validation_errors
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM validated)
)

SELECT line
FROM (
    SELECT 0 AS sort_order,
           '"row_type","employNum","schoolNum","startDate","assignmentCode","validation_errors"' AS line
    FROM dual

    UNION ALL

    SELECT 1,
           '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
           '"'||REPLACE(NVL(employNum,''),'"','""')||'",'||
           '"'||REPLACE(NVL(schoolNum,''),'"','""')||'",'||
           '"'||NVL(TO_CHAR(startDate,'MM/DD/YYYY'),'')||'",'||
           '"'||REPLACE(NVL(assignmentCode,''),'"','""')||'",'||
           '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
    FROM final_rows
)
ORDER BY sort_order;