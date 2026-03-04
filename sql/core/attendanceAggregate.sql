/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: attendanceAggregate.csv

  Required Columns: studentNum, date
  Unique Key: studentNum, date

  Business Rules:
  - Years-of-data rule: 7.
  - Always emit at least one row (ERROR) when no source rows are found so district DQ review is never "silent empty".

  Oracle SQL
============================================================================ */

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

/* Student scope: students active within the 7-year window */
student_scope AS (
  SELECT DISTINCT
    s.id AS studentid,
    CAST(s.student_number AS VARCHAR2(15)) AS studentNum
  FROM ps.students s
  JOIN ps.cc cc
    ON cc.studentid = s.id
  JOIN active_yearid ay
    ON FLOOR(cc.termid/100) BETWEEN (ay.yearid - 6) AND ay.yearid
  WHERE s.student_number IS NOT NULL
),

/* REAL ATTENDANCE SOURCE */
ps_source AS (
  SELECT
    ss.studentNum,
    a.att_date AS agg_date
  FROM student_scope ss
  JOIN ps.attendance a
    ON a.studentid = ss.studentid
  WHERE a.att_date IS NOT NULL
),

validated AS (
  SELECT
    studentNum,
    agg_date,
    TRIM(
      CASE WHEN studentNum IS NULL OR TRIM(studentNum) = '' THEN 'ERR_REQUIRED_studentNum; ' END ||
      CASE WHEN agg_date IS NULL THEN 'ERR_REQUIRED_date; ' END
    ) AS validation_errors
  FROM ps_source
),

final_rows AS (
  SELECT
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END AS row_type,
    studentNum,
    agg_date,
    validation_errors
  FROM validated

  UNION ALL

  SELECT
    'ERROR',
    NULL,
    NULL,
    'ERR_NO_SOURCE_ROWS_FOR_ATTENDANCEAGGREGATE; '
  FROM dual
  WHERE NOT EXISTS (SELECT 1 FROM validated)
)

SELECT line
FROM (
    SELECT 0 AS sort_order,
           '"row_type","studentNum","date","validation_errors"' AS line
    FROM dual

    UNION ALL

    SELECT 1,
           '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
           '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
           '"'||NVL(TO_CHAR(agg_date,'MM/DD/YYYY'),'')||'",'||
           '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
    FROM final_rows
)
ORDER BY sort_order;
