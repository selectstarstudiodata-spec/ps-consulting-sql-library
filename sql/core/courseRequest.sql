/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: courseRequest.csv

   Required Columns (common IC pattern):
   - studentNumber
   - courseNumber
   - schoolNum

   Unique Key (recommended):
   - studentNumber, courseNumber, schoolNum

   Constraints:
   - studentNumber must exist in student.csv
   - courseNumber must exist in course.csv
   - schoolNum must exist in school.csv

   BUSINESS RULE (ND conversion plan):
   - Schedules / courseRequest are 1 year only (trial/prod year).
============================================================================ */

WITH
school_key_map AS (
    SELECT
        school_number AS school_key,
        school_number AS schoolNum,
        name AS schoolName
    FROM ps.schools
),

active_year AS (
    SELECT yearid AS active_yearid
    FROM (
        SELECT
          t.yearid,
          ROW_NUMBER() OVER (
            ORDER BY
              CASE WHEN SYSDATE BETWEEN t.firstday AND t.lastday THEN 0 ELSE 1 END,
              NVL(t.lastday, DATE '1900-01-01') DESC,
              t.id DESC
          ) rn
        FROM ps.terms t
        WHERE t.isyearrec = 1
    )
    WHERE rn = 1
),

year_term AS (
    SELECT
      skm.schoolNum,
      skm.schoolName,
      t.firstday,
      t.lastday
    FROM ps.terms t
    JOIN school_key_map skm
      ON skm.school_key = t.schoolid
    WHERE t.isyearrec = 1
      AND t.yearid = (SELECT active_yearid FROM active_year)
),

calendar_final AS (
  SELECT
    schoolNum,
    SUBSTR(TO_CHAR(EXTRACT(YEAR FROM firstday)),-2) || '-' ||
    SUBSTR(TO_CHAR(EXTRACT(YEAR FROM lastday)),-2) || ' ' ||
    SUBSTR(TRIM(schoolName),1,23) AS calendarName
  FROM year_term
),

active_secs AS (
  SELECT
    sec.id AS sectionid,
    sec.course_number,
    sec.schoolid
  FROM ps.sections sec
  JOIN ps.terms t
    ON t.id = sec.termid
   AND t.schoolid = sec.schoolid
  WHERE t.yearid = (SELECT active_yearid FROM active_year)
),

ps_source AS (
  SELECT DISTINCT
    CAST(TO_CHAR(st.student_number) AS VARCHAR2(20)) AS studentNumber_raw,
    CAST(TRIM(a.course_number) AS VARCHAR2(20)) AS courseNumber_raw,
    CAST(TO_CHAR(skm.schoolNum) AS VARCHAR2(7)) AS schoolNum_raw,
    cf.calendarName AS calendarName_raw,
    CAST(NULL AS NUMBER(1)) AS exclude_raw
  FROM ps.cc cc
  JOIN active_secs a
    ON a.sectionid = cc.sectionid
  JOIN ps.students st
    ON st.id = cc.studentid
  JOIN school_key_map skm
    ON skm.school_key = a.schoolid
  JOIN calendar_final cf
    ON cf.schoolNum = skm.schoolNum
  WHERE st.student_number IS NOT NULL
    AND skm.schoolNum IS NOT NULL
),

xform AS (
  SELECT
    SUBSTR(TRIM(studentNumber_raw),1,20) AS studentNumber,
    SUBSTR(TRIM(courseNumber_raw),1,20) AS courseNumber,
    SUBSTR(TRIM(schoolNum_raw),1,7) AS schoolNum,
    SUBSTR(TRIM(calendarName_raw),1,30) AS calendarName,
    exclude_raw AS exclude
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN studentNumber IS NULL OR TRIM(studentNumber) = '' THEN 'ERR_REQUIRED_studentNumber; ' END ||
      CASE WHEN courseNumber IS NULL OR TRIM(courseNumber) = '' THEN 'ERR_REQUIRED_courseNumber; ' END ||
      CASE WHEN schoolNum IS NULL OR TRIM(schoolNum) = '' THEN 'ERR_REQUIRED_schoolNum; ' END ||
      CASE WHEN calendarName IS NULL OR TRIM(calendarName) = '' THEN 'ERR_REQUIRED_calendarName; ' END
    ) AS validation_errors
  FROM xform x
)

SELECT
  '"row_type","studentNumber","courseNumber","schoolNum","calendarName","exclude","validation_errors"'
FROM dual

UNION ALL

SELECT
  '"'||CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END||'",'||
  '"'||NVL(studentNumber,'')||'",'||
  '"'||NVL(courseNumber,'')||'",'||
  '"'||NVL(schoolNum,'')||'",'||
  '"'||REPLACE(NVL(calendarName,''),'"','""')||'",'||
  '"'||NVL(exclude,'')||'",'||
  '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
FROM validated;
