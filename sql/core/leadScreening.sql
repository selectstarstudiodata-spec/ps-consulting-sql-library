/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: leadScreening.csv

  Required Columns: studentNum, date
  Unique Key: studentNum, date

  Business Rules:
  - Years-of-data rule: *** all records.
  - Always emit at least one row (ERROR) when no source rows are found so district DQ review is never "silent empty".
  - Source mapping is intentionally conservative: it prefers producing reviewable rows (with validation_errors)
    over dropping records.

  Oracle SQL
============================================================================ */

set heading off
set pagesize 0
set feedback off
set underline off

WITH student_scope AS (
  SELECT
    s.id AS studentid,
    CAST(s.student_number AS VARCHAR2(15)) AS studentNum
  FROM ps.students s
  WHERE s.student_number IS NOT NULL
),

ps_source AS (
  SELECT
    ss.studentNum,
    CAST(a.dateexpires AS DATE) AS screening_date
  FROM student_scope ss
  JOIN ps.psm_studentalert a
    ON a.studentid = ss.studentid
  WHERE a.dateexpires IS NOT NULL
    AND (
         UPPER(a.description) LIKE '%LEAD%'
      OR UPPER(a.description) LIKE '%PB%'
      OR UPPER(a.description) LIKE '%SCREEN%'
    )
),

validated AS (
  SELECT
    CAST(studentNum AS VARCHAR2(15)) AS studentNum,
    CAST(screening_date AS DATE) AS screening_date,
    CAST(
      TRIM(
        CASE WHEN studentNum IS NULL OR TRIM(studentNum) = '' THEN 'ERR_REQUIRED_studentNum; ' END ||
        CASE WHEN screening_date IS NULL THEN 'ERR_REQUIRED_date; ' END
      )
    AS VARCHAR2(200)) AS validation_errors
  FROM ps_source
),

final_rows AS (
  SELECT
    2 AS sort_order,
    CAST(CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END AS VARCHAR2(10)) AS row_type,
    studentNum,
    screening_date,
    validation_errors
  FROM validated

  UNION ALL

  SELECT
    2,
    CAST('ERROR' AS VARCHAR2(10)),
    CAST(NULL AS VARCHAR2(15)),
    CAST(NULL AS DATE),
    CAST('ERR_NO_SOURCE_ROWS_FOR_LEADSCREENING; ' AS VARCHAR2(200))
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM validated)
)

SELECT csv_line
FROM (
  SELECT
    1 AS sort_order,
    '"row_type","studentNum","date","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    sort_order,
    '"'||
    REPLACE(NVL(row_type,''),'"','""')||'",'||
    '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
    '"'||NVL(TO_CHAR(screening_date,'MM/DD/YYYY'),'')||'",'||
    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
  FROM final_rows
)
ORDER BY sort_order;