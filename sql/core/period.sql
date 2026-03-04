set heading off
set pagesize 0
set feedback off
set underline off

/* ============================================================================
   period.csv  (STRUCTURALLY CORRECTED)
   - Anchored to calendar_day (inSession = 1)
   - Uses DISTINCT (cycle_day_id, bell_schedule_id)
   - Prevents artificial schedule inflation
   - Times tied to actual bell schedule used during the year
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
    t.id     AS termid,
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

/* TRUE structural combinations from calendar_day */
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

period_truth AS (
  SELECT
    skm.schoolNum,
    p.schoolid,
    p.id AS period_id,
    p.period_number AS periodSeq,
    COALESCE(
      NULLIF(TRIM(p.abbreviation),''),
      NULLIF(TRIM(p.name),''),
      'P'||p.period_number
    ) AS periodName
  FROM ps.period p
  JOIN school_key_map skm
    ON skm.school_key = p.schoolid
),

bell_times AS (
  SELECT
    bs.schoolid,
    bs.year_id,
    bsi.period_id,
    bsi.bell_schedule_id,
    LPAD(TRUNC(bsi.start_time/3600),2,'0') || ':' ||
    LPAD(TRUNC(MOD(bsi.start_time,3600)/60),2,'0') AS startTime,
    LPAD(TRUNC(bsi.end_time/3600),2,'0') || ':' ||
    LPAD(TRUNC(MOD(bsi.end_time,3600)/60),2,'0') AS endTime
  FROM ps.bell_schedule bs
  JOIN ps.bell_schedule_items bsi
    ON bsi.bell_schedule_id = bs.id
  WHERE bs.year_id IN (SELECT yearid FROM yearrec_terms)
),

final_truth AS (
  SELECT
    cf.schoolNum,
    cf.calendarName,
    cdm.scheduleName,
    pt.periodName,
    pt.periodSeq,
    bt.startTime,
    bt.endTime
  FROM calendar_final cf
  JOIN calendar_schedule_pairs csp
    ON csp.schoolid = cf.schoolid
  JOIN cycle_day_map cdm
    ON cdm.schoolNum    = cf.schoolNum
   AND cdm.year_id      = cf.yearid
   AND cdm.cycle_day_id = csp.cycle_day_id
  JOIN period_truth pt
    ON pt.schoolid = cf.schoolid
  JOIN bell_times bt
    ON bt.schoolid        = cf.schoolid
   AND bt.year_id         = cf.yearid
   AND bt.period_id       = pt.period_id
   AND bt.bell_schedule_id = csp.bell_schedule_id
)

/* HEADER */
SELECT
  '"row_type","schoolNum","calendarName","scheduleName","periodName","periodSeq","startTime","endTime","validation_errors"'
FROM dual

UNION ALL

/* DATA */
SELECT
  '"CLEAN",'||
  '"'||schoolNum||'",'||
  '"'||REPLACE(calendarName,'"','""')||'",'||
  '"'||REPLACE(scheduleName,'"','""')||'",'||
  '"'||REPLACE(periodName,'"','""')||'",'||
  '"'||periodSeq||'",'||
  '"'||NVL(startTime,'')||'",'||
  '"'||NVL(endTime,'')||'",'||
  '""'
FROM final_truth;