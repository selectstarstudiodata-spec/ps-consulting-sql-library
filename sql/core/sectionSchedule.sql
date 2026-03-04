set heading off
set pagesize 0
set feedback off
set underline off

/* ============================================================================
  ETL: PowerSchool -> Infinite Campus
  Target file: sectionSchedule.csv
  FIXED: Uses SECTION_MEETING + CYCLE_DAY (no bell schedule fabrication)
============================================================================ */

WITH school_key_map AS (
  SELECT school_number AS school_key,
         school_number AS schoolNum,
         name AS schoolName
  FROM ps.schools
  WHERE school_number IS NOT NULL
    AND school_number <> 999999
),

yearrec_terms AS (
  SELECT skm.schoolNum,
         skm.schoolName,
         t.id     AS termid,
         t.yearid,
         t.firstday,
         t.lastday,
         t.schoolid
  FROM ps.terms t
  JOIN school_key_map skm
    ON skm.school_key = t.schoolid
  WHERE t.isyearrec = 1
    AND t.firstday >= DATE '2025-07-01'
    AND t.firstday <  DATE '2026-07-01'
),

calendar_final AS (
  SELECT schoolNum,
         yearid,
         schoolid,
         SUBSTR(TO_CHAR(EXTRACT(YEAR FROM firstday)),-2) || '-' ||
         SUBSTR(TO_CHAR(EXTRACT(YEAR FROM lastday)),-2) || ' ' ||
         SUBSTR(TRIM(schoolName),1,23) AS calendarName
  FROM yearrec_terms
),

sec_term AS (
  SELECT sec.id AS sectionid,
         sec.section_number,
         sec.course_number,
         sec.schoolid,
         t.name AS termName,
         t.yearid
  FROM ps.sections sec
  JOIN ps.terms t
    ON t.id = sec.termid
   AND t.schoolid = sec.schoolid
  WHERE t.isyearrec = 1
    AND t.firstday >= DATE '2025-07-01'
    AND t.firstday <  DATE '2026-07-01'
),

/* REAL cycle day names */
cycle_day_map AS (
  SELECT
         skm.schoolNum,
         cd.year_id,
         cd.letter,
         COALESCE(
           NULLIF(TRIM(cd.day_name),''),
           NULLIF(TRIM(cd.letter),''),
           'Day '||cd.day_number
         ) AS scheduleName
  FROM ps.cycle_day cd
  JOIN school_key_map skm
    ON skm.school_key = cd.schoolid
  WHERE cd.year_id IN (SELECT yearid FROM yearrec_terms)
),

/* REAL section meeting truth */
section_meetings AS (
  SELECT
         sm.sectionid,
         sm.period_number,
         sm.cycle_day_letter
  FROM ps.section_meeting sm
),

/* Final authoritative schedule rows */
section_schedule_truth AS (
  SELECT
         st.sectionid,
         st.section_number,
         st.course_number,
         st.termName,
         skm.schoolNum,
         cf.calendarName,
         cdm.scheduleName,
         sm.period_number
  FROM sec_term st
  JOIN school_key_map skm
    ON skm.school_key = st.schoolid
  JOIN calendar_final cf
    ON cf.schoolid = st.schoolid
  JOIN section_meetings sm
    ON sm.sectionid = st.sectionid
  JOIN cycle_day_map cdm
    ON cdm.schoolNum = skm.schoolNum
   AND cdm.year_id   = st.yearid
   AND cdm.letter    = sm.cycle_day_letter
)

/* HEADER ROW */
SELECT '"row_type","schoolNum","calendarName","courseNum","sectionNum","startTermName","endTermName","scheduleName","startPeriodName","endPeriodName","exclude","validation_errors"'
FROM dual

UNION ALL

/* DATA ROWS */
SELECT
  '"CLEAN",'||
  '"'||schoolNum||'",'||
  '"'||calendarName||'",'||
  '"'||course_number||'",'||
  '"'||section_number||'",'||
  '"'||termName||'",'||
  '"'||termName||'",'||
  '"'||scheduleName||'",'||
  '"P'||period_number||'",'||
  '"P'||period_number||'",'||
  '"",'||
  '""'
FROM section_schedule_truth;