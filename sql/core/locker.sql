/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: locker.csv

  Required Columns: schoolNum, lockerNum
  Unique Key: schoolNum, lockerNum

  Business Rules:
  - Years-of-data rule: 1 active year
  - District does NOT use the Locker module in PowerSchool
  - Always emit one ERROR row so file is never empty

  Oracle SQL
============================================================================ */

set heading off
set pagesize 0
set feedback off
set underline off

WITH active_year AS (
  SELECT MAX(t.yearid) AS active_yearid
  FROM ps.sections sec
  JOIN ps.terms t ON t.id = sec.termid
),

student_scope AS (
  SELECT DISTINCT
    s.id AS studentid,
    s.schoolid,
    CAST(s.student_number AS VARCHAR2(15)) AS studentNum
  FROM ps.students s
  JOIN ps.cc cc        ON cc.studentid = s.id
  JOIN ps.sections sec ON sec.id = cc.sectionid
  JOIN ps.terms t      ON t.id = sec.termid
  WHERE t.yearid = (SELECT active_yearid FROM active_year)
    AND s.student_number IS NOT NULL
),

ps_source AS (
  SELECT
    CAST(NULL AS VARCHAR2(7))  AS schoolNum,
    CAST(NULL AS VARCHAR2(10)) AS lockerNum,
    CAST(NULL AS VARCHAR2(3))  AS lockerType,
    CAST(NULL AS VARCHAR2(15)) AS studentNum
  FROM dual
  WHERE 1 = 0
),

validated AS (
  SELECT
    schoolNum,
    lockerNum,
    lockerType,
    studentNum,
    'ERR_NO_SOURCE_ROWS_FOR_LOCKER; ' AS validation_errors
  FROM ps_source
),

final_rows AS (
  SELECT
    2 AS sort_order,
    'ERROR' AS row_type,
    NULL AS schoolNum,
    NULL AS lockerNum,
    NULL AS lockerType,
    NULL AS studentNum,
    'ERR_NO_SOURCE_ROWS_FOR_LOCKER; ' AS validation_errors
  FROM dual
)

SELECT csv_line
FROM (
  SELECT
    1 AS sort_order,
    '"row_type","schoolNum","lockerNum","lockerType","studentNum","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    sort_order,
    '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
    '"'||REPLACE(NVL(schoolNum,''),'"','""')||'",'||
    '"'||REPLACE(NVL(lockerNum,''),'"','""')||'",'||
    '"'||REPLACE(NVL(lockerType,''),'"','""')||'",'||
    '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
  FROM final_rows
)
ORDER BY sort_order;