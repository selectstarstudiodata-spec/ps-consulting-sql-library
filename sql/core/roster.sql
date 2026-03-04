/* ============================================================================
/* ============================================================================
   REVISION SUMMARY – ROSTER EXTRACT UPDATE

   Changes Made:

   1. Removed schedulecc UNION
      - Eliminated UNION ALL with ps.schedulecc in all_cc.
      - Extract now pulls committed schedule data exclusively from ps.cc.
      - Prevents inclusion of stale PowerScheduler data.

   2. Preserved Dropped Class Enrollments
      - Applied ABS() to ps.cc.sectionid.
      - Ensures negative sectionid values (dropped classes) are included.
      - Maintains historical enrollment accuracy.

   3. Removed cc_minmax Aggregation
      - Deleted cc_minmax CTE.
      - Removed MIN/MAX consolidation of enrollment dates.
      - Each enrollment event is now represented as its own row.
      - Prevents collapsing multiple enrollments into a single continuous span.

   Result:
      - Extract reflects true enrollment history.
      - Dropped and re-enrolled scenarios are preserved accurately.
      - Logic aligned with PowerSchool committed-year data behavior.
============================================================================ */
============================================================================ */
set heading off
set pagesize 0
set feedback off
set underline off
set newpage none

WITH
school_key_map AS (
    SELECT
        school_number AS school_key,
        school_number AS schoolNum,
        name AS schoolName
    FROM ps.schools
),

yearrec_terms AS (
  SELECT
    skm.schoolNum,
    skm.schoolName,
    t.firstday,
    t.lastday,
    t.schoolid,
    t.yearid
  FROM ps.terms t
  JOIN school_key_map skm
    ON skm.school_key = t.schoolid
  WHERE t.isyearrec = 1
    AND t.firstday >= DATE '2025-07-01'
    AND t.firstday <  DATE '2026-07-01'
    AND skm.schoolNum IS NOT NULL
    AND skm.schoolNum <> 999999
),

calendar_final AS (
  SELECT
    schoolNum,
    schoolid,
    firstday,
    lastday,
    yearid,
    SUBSTR(TO_CHAR(EXTRACT(YEAR FROM firstday)),-2) || '-' ||
    SUBSTR(TO_CHAR(EXTRACT(YEAR FROM lastday)),-2) || ' ' ||
    SUBSTR(TRIM(schoolName),1,23) AS calendarName
  FROM yearrec_terms
),

active_sections AS (
  SELECT
    sec.id,
    sec.schoolid
  FROM ps.sections sec
  JOIN ps.terms t
    ON t.id = sec.termid
   AND t.schoolid = sec.schoolid
  WHERE t.isyearrec = 1
    AND t.firstday >= DATE '2025-07-01'
    AND t.firstday <  DATE '2026-07-01'
),

all_cc AS (
    SELECT
        ABS(cc.sectionid) AS sectionid,
        cc.studentid,
        cc.dateenrolled,
        cc.dateleft
    FROM ps.cc cc
    JOIN active_sections a
      ON a.id = ABS(cc.sectionid)
    WHERE cc.studentid IS NOT NULL
      AND cc.sectionid IS NOT NULL
      AND cc.dateenrolled IS NOT NULL
),

ps_source AS (
  SELECT
    CAST(TO_CHAR(sec.section_number) AS VARCHAR2(20)) AS sectionNumber_raw,
    CAST(TO_CHAR(st.student_number) AS VARCHAR2(20)) AS studentNumber_raw,
    CAST(TO_CHAR(sec.course_number) AS VARCHAR2(30)) AS course_number_raw,
    CAST(TO_CHAR(skm.schoolNum) AS VARCHAR2(10)) AS schoolID_raw,
    cf.calendarName AS calendarName_raw,
    c.dateenrolled AS startDate_dt,
    c.dateleft     AS endDate_dt,
    CAST(NULL AS NUMBER(1)) AS exclude_raw
  FROM all_cc c
  JOIN ps.students st ON st.id = c.studentid
  JOIN ps.sections sec ON sec.id = c.sectionid
  JOIN active_sections a ON a.id = sec.id
  LEFT JOIN school_key_map skm ON skm.school_key = sec.schoolid
  JOIN calendar_final cf
    ON cf.schoolid = sec.schoolid
   AND c.dateenrolled <= cf.lastday
   AND NVL(c.dateleft, cf.lastday) >= cf.firstday
  WHERE st.student_number IS NOT NULL
    AND sec.section_number IS NOT NULL
),

xform AS (
  SELECT
    SUBSTR(TRIM(sectionNumber_raw),1,20) AS sectionNumber,
    SUBSTR(TRIM(studentNumber_raw),1,20) AS studentNumber,
    SUBSTR(TRIM(course_number_raw),1,30) AS course_number,
    SUBSTR(TRIM(schoolID_raw),1,10) AS schoolID,
    SUBSTR(TRIM(calendarName_raw),1,30) AS calendarName,
    startDate_dt,
    endDate_dt,
    exclude_raw AS exclude
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN sectionNumber IS NULL OR TRIM(sectionNumber) = '' THEN 'ERR_REQUIRED_sectionNumber; ' END ||
      CASE WHEN studentNumber IS NULL OR TRIM(studentNumber) = '' THEN 'ERR_REQUIRED_studentNumber; ' END ||
      CASE WHEN startDate_dt IS NULL THEN 'ERR_REQUIRED_startDate; ' END ||
      CASE WHEN calendarName IS NULL OR TRIM(calendarName) = '' THEN 'ERR_REQUIRED_calendarName; ' END
    ) AS validation_errors
  FROM xform x
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
         '"row_type","sectionNum","studentNum","courseNum","schoolNum","calendarName","startDate","endDate","exclude","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT 1,
         ROW_NUMBER() OVER (ORDER BY sectionNumber, studentNumber, startDate_dt),

         '"'||
         CASE WHEN validation_errors IS NULL OR validation_errors = ''
              THEN 'CLEAN'
              ELSE 'ERROR'
         END||'",'||

         '"'||REPLACE(NVL(sectionNumber,''),'"','""')||'",'||
         '"'||REPLACE(NVL(studentNumber,''),'"','""')||'",'||
         '"'||REPLACE(NVL(course_number,''),'"','""')||'",'||
         '"'||REPLACE(NVL(schoolID,''),'"','""')||'",'||
         '"'||REPLACE(NVL(calendarName,''),'"','""')||'",'||
         '"'||NVL(TO_CHAR(startDate_dt,'MM/DD/YYYY'),'')||'",'||
         '"'||NVL(TO_CHAR(endDate_dt,'MM/DD/YYYY'),'')||'",'||
         '"'||NVL(TO_CHAR(exclude),'')||'",'||
         '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'

  FROM validated
)
ORDER BY sort_order, data_order;