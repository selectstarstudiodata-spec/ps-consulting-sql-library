set heading off
set pagesize 0
set feedback off
set underline off
set newpage none

/* ============================================================================
   ETL: PowerSchool -> Infinite Campus
   Target file: enrollment_26-27.csv
============================================================================ */

WITH
active_year AS (
    SELECT MAX(t.yearid) AS active_yearid
    FROM ps.sections sec
    JOIN ps.terms t ON t.id = sec.termid
),

target_year AS (
    SELECT (SELECT active_yearid FROM active_year) + 1 AS target_yearid
    FROM dual
),

active_students AS (
  SELECT DISTINCT
    st.id   AS studentid,
    st.dcid AS studentdcid,
    st.student_number,
    st.enroll_status,
    st.next_school,
    st.sched_nextyeargrade
  FROM ps.students st
  JOIN ps.cc cc ON cc.studentid = st.id
  JOIN ps.sections sec ON sec.id = cc.sectionid
  JOIN ps.terms t ON t.id = sec.termid
  WHERE t.yearid = (SELECT active_yearid FROM active_year)
    AND st.student_number IS NOT NULL
    AND st.student_number <> 0
),

eligible_students AS (
  SELECT *
  FROM active_students
  WHERE enroll_status = 0
    AND next_school IS NOT NULL
    AND next_school NOT IN (0, 999999)
),

next_school_resolved AS (
  SELECT
    es.*,
    sch.id AS next_schoolid,
    sch.school_number AS next_schoolnum
  FROM eligible_students es
  LEFT JOIN ps.schools sch
    ON sch.school_number = es.next_school
),

next_school_year_term AS (
  SELECT
    t.schoolid,
    MIN(t.firstday) AS year_firstday,
    MAX(t.lastday)  AS year_lastday
  FROM ps.terms t
  WHERE t.yearid = (SELECT target_yearid FROM target_year)
  GROUP BY t.schoolid
),

ps_source AS (
  SELECT
    CAST(TO_CHAR(nsr.student_number) AS VARCHAR2(15)) AS studentNum_raw,
    CAST(TO_CHAR(nsr.next_schoolnum) AS VARCHAR2(20)) AS schoolNum_raw,
    CAST(TRIM(TO_CHAR(nsr.sched_nextyeargrade)) AS VARCHAR2(20)) AS grade_raw,

    (SELECT nsy.year_firstday
     FROM next_school_year_term nsy
     WHERE nsy.schoolid = nsr.next_schoolid) AS startDate_dt,

    (SELECT nsy.year_lastday
     FROM next_school_year_term nsy
     WHERE nsy.schoolid = nsr.next_schoolid) AS endDate_dt,

    CAST('Active' AS VARCHAR2(20)) AS enrollmentStatus_raw,

    CAST(
      'TRIAL_ONLY_PSEUDO_ROLLOVER_26_27: Derived from Students.NEXT_SCHOOL (treated as Schools.school_number) ' ||
      'and Students.SCHED_NEXTYEARGRADE. Excludes NEXT_SCHOOL in (0, 999999). ' ||
      'Calendar dates pulled from Terms for target year using resolved school; NULL dates require district cleanup.'
      AS VARCHAR2(255)
    ) AS comments_raw,

    CAST(NULL AS NUMBER(1)) AS exclude_raw

  FROM next_school_resolved nsr
),

xform AS (
  SELECT
    SUBSTR(TRIM(studentNum_raw), 1, 15) AS studentNum,
    SUBSTR(TRIM(schoolNum_raw),  1, 20) AS schoolNum,
    SUBSTR(TRIM(grade_raw),      1, 20) AS grade,
    startDate_dt,
    endDate_dt,
    SUBSTR(TRIM(enrollmentStatus_raw), 1, 20) AS enrollmentStatus,
    SUBSTR(TRIM(comments_raw), 1, 255) AS comments,
    exclude_raw AS exclude
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN studentNum IS NULL OR TRIM(studentNum) = '' THEN 'ERR_REQUIRED_studentNum; ' END ||
      CASE WHEN schoolNum  IS NULL OR TRIM(schoolNum)  = '' THEN 'ERR_REQUIRED_schoolNum_from_NEXT_SCHOOL; ' END ||
      CASE WHEN grade      IS NULL OR TRIM(grade)      = '' THEN 'WARN_MISSING_SCHED_NEXTYEARGRADE; ' END ||
      CASE WHEN startDate_dt IS NULL THEN 'WARN_MISSING_26_27_calendar_startDate; ' END ||
      CASE WHEN endDate_dt   IS NULL THEN 'WARN_MISSING_26_27_calendar_endDate; ' END
    ) AS validation_errors
  FROM xform x
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
         '"record_status","studentNum","schoolNum","grade","startDate","endDate","enrollmentStatus","comments","exclude","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT 1,
         ROW_NUMBER() OVER (ORDER BY studentNum, schoolNum),

         '"'||
         CASE
           WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN'
           WHEN INSTR(validation_errors, 'ERR_REQUIRED_schoolNum_from_NEXT_SCHOOL') > 0 THEN 'ERROR'
           WHEN INSTR(validation_errors, 'ERR_REQUIRED_studentNum') > 0 THEN 'ERROR'
           ELSE 'CLEAN'
         END||'",'||

         '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
         '"'||REPLACE(NVL(schoolNum,''),'"','""')||'",'||
         '"'||REPLACE(NVL(grade,''),'"','""')||'",'||
         '"'||NVL(TO_CHAR(startDate_dt,'MM/DD/YYYY'),'')||'",'||
         '"'||NVL(TO_CHAR(endDate_dt,'MM/DD/YYYY'),'')||'",'||
         '"'||REPLACE(NVL(enrollmentStatus,''),'"','""')||'",'||
         '"'||REPLACE(NVL(comments,''),'"','""')||'",'||
         '"'||NVL(TO_CHAR(exclude),'')||'",'||
         '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'

  FROM validated
)
ORDER BY sort_order, data_order;