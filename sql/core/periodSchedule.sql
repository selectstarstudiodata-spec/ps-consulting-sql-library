

set heading off
set pagesize 0
set feedback off
set underline off

/* ============================================================================
   periodSchedule.csv  
   FIXED:
   - Uses distinct (cycle_day_id, bell_schedule_id) from calendar_day
   - Prevents noise from arbitrary calendar iterations
   - Guarantees schedules only when inSession = 1
   - No fabricated “most common” logic
============================================================================ */

WITH

school_key_map AS (
  SELECT
    school_number AS school_key,
    school_number AS schoolNum,
    name AS schoolName
  FROM ps.schools
  WHERE school_number IS NOT NULL
    AND school_number <> 999999
),

yearrec_terms AS (
  SELECT
    skm.schoolNum,
    skm.schoolName,
    t.id     AS year_termid,
    t.yearid AS yearid,
    t.firstday,
    t.lastday,
    t.schoolid
  FROM ps.terms t
  JOIN school_key_map skm
    ON skm.school_key = t.schoolid
  WHERE t.isyearrec = 1
    AND t.firstday >= DATE '2025-07-01'
    AND t.firstday <  DATE '2026-07-01'
    AND NOT REGEXP_LIKE(LOWER(t.name),
        '(summer| ss|camp|edventure|home school|non-public|non public)')
),

calendar_final AS (
  SELECT
    schoolNum,
    yearid,
    schoolid,
    SUBSTR(TO_CHAR(EXTRACT(YEAR FROM firstday)),-2) || '-' ||
    SUBSTR(TO_CHAR(EXTRACT(YEAR FROM lastday)),-2) || ' ' ||
    SUBSTR(TRIM(schoolName),1,23) AS calendarName
  FROM yearrec_terms
),

/* TRUE structural schedule combinations from calendar_day */
calendar_schedule_pairs AS (
  SELECT DISTINCT
         cd.schoolid,
         cd.cycle_day_id,
         cd.bell_schedule_id
  FROM ps.calendar_day cd
  JOIN yearrec_terms yt
    ON yt.schoolid = cd.schoolid
   AND cd.date_value BETWEEN yt.firstday AND yt.lastday
  WHERE cd.insession = 1
    AND cd.cycle_day_id IS NOT NULL
    AND cd.bell_schedule_id IS NOT NULL
),

cycle_day_map AS (
  SELECT
    skm.schoolNum,
    cd.year_id,
    cd.id AS cycle_day_id,
    COALESCE(
      NULLIF(TRIM(cd.day_name),''),
      NULLIF(TRIM(cd.letter),''),
      NULLIF(TRIM(cd.abbreviation),''),
      'Day '||cd.day_number
    ) AS scheduleName
  FROM ps.cycle_day cd
  JOIN school_key_map skm
    ON skm.school_key = cd.schoolid
),

xform AS (
  SELECT
    cf.schoolNum,
    cf.calendarName,
    cdm.scheduleName,
    ROW_NUMBER() OVER (
      PARTITION BY cf.schoolNum, cf.calendarName
      ORDER BY cdm.scheduleName
    ) AS scheduleSeq
  FROM calendar_final cf
  JOIN calendar_schedule_pairs csp
    ON csp.schoolid = cf.schoolid
  JOIN cycle_day_map cdm
    ON cdm.schoolNum     = cf.schoolNum
   AND cdm.year_id       = cf.yearid
   AND cdm.cycle_day_id  = csp.cycle_day_id
)

/* HEADER */
SELECT
  '"row_type","schoolNum","calendarName","scheduleName","scheduleSeq","validation_errors"'
FROM dual

UNION ALL

/* DATA */
SELECT
  '"CLEAN",'||
  '"'||schoolNum||'",'||
  '"'||REPLACE(calendarName,'"','""')||'",'||
  '"'||REPLACE(scheduleName,'"','""')||'",'||
  '"'||scheduleSeq||'",'||
  '""'
FROM xform;