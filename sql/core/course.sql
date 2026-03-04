/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: course.csv
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

yearrec_term AS (
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
      AND skm.schoolNum IS NOT NULL
),

calendar_final AS (
    SELECT
        schoolNum,
        SUBSTR(TO_CHAR(EXTRACT(YEAR FROM firstday)),-2) || '-' ||
        SUBSTR(TO_CHAR(EXTRACT(YEAR FROM lastday)),-2) || ' ' ||
        SUBSTR(TRIM(schoolName),1,23) AS calendarName
    FROM yearrec_term
),

active_sections AS (
  SELECT DISTINCT
    skm.schoolNum AS schoolNum_raw,
    sec.course_number AS courseNumber_raw
  FROM ps.sections sec
  JOIN ps.terms t
    ON t.id = sec.termid
   AND t.schoolid = sec.schoolid
  JOIN school_key_map skm
    ON skm.school_key = sec.schoolid
  WHERE t.yearid = (SELECT active_yearid FROM active_year)
    AND sec.course_number IS NOT NULL
    AND skm.schoolNum IS NOT NULL
),

ps_source AS (
  SELECT
    a.schoolNum_raw,
    cf.calendarName,
    c.course_number AS courseNumber_raw,
    c.course_name   AS courseName_raw,
    CAST(NULL AS VARCHAR2(50)) AS subject_raw,
    CAST(NULL AS VARCHAR2(10)) AS credit_raw,
    CAST(NULL AS NUMBER(1))    AS exclude_raw,
    1 AS addToCatalog_raw
  FROM active_sections a
  JOIN ps.courses c
    ON c.course_number = a.courseNumber_raw
  JOIN calendar_final cf
    ON cf.schoolNum = a.schoolNum_raw
),

xform AS (
  SELECT
    SUBSTR(TRIM(schoolNum_raw),1,7)       AS schoolNum,
    SUBSTR(TRIM(calendarName),1,30)       AS calendarName,
    SUBSTR(TRIM(courseNumber_raw),1,20)   AS courseNumber,
    SUBSTR(TRIM(courseName_raw),1,100)    AS courseName,
    subject_raw                          AS subject,
    credit_raw                           AS credit,
    exclude_raw                          AS exclude,
    addToCatalog_raw                     AS addToCatalog
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN schoolNum IS NULL OR TRIM(schoolNum) = '' THEN 'ERR_REQUIRED_schoolNum; ' END ||
      CASE WHEN calendarName IS NULL OR TRIM(calendarName) = '' THEN 'ERR_REQUIRED_calendarName; ' END ||
      CASE WHEN courseNumber IS NULL OR TRIM(courseNumber) = '' THEN 'ERR_REQUIRED_courseNumber; ' END ||
      CASE WHEN courseName IS NULL OR TRIM(courseName) = '' THEN 'ERR_REQUIRED_courseName; ' END
    ) AS validation_errors
  FROM xform x
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order,
         '"row_type","schoolNum","calendarName","courseNum","courseName","subject","credit","exclude","addToCatalog","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    1 AS sort_order,
    '"' || CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END || '",' ||
    '"' || NVL(schoolNum,'') || '",' ||
    '"' || REPLACE(NVL(calendarName,''),'"','""') || '",' ||
    '"' || NVL(courseNumber,'') || '",' ||
    '"' || REPLACE(NVL(courseName,''),'"','""') || '",' ||
    '"' || REPLACE(NVL(subject,''),'"','""') || '",' ||
    '"' || NVL(credit,'') || '",' ||
    '"' || NVL(exclude,'') || '",' ||
    '"' || NVL(addToCatalog,'') || '",' ||
    '"' || REPLACE(NVL(validation_errors,''),'"','""') || '"'
  FROM validated
)
ORDER BY sort_order;
