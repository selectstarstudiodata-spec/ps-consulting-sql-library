set heading off
set pagesize 0
set feedback off
set underline off
set newpage none

/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: room.csv

   Required Columns:
   - roomNumber
   - schoolNum

   Unique Key:
   - roomNumber, schoolNum

   Constraints:
   - schoolNum must exist in school.csv

   NOTE:
   - This follows the SAME room sourcing pattern as section.sql:
     room comes from ps.schedulesections (authoritative),
     joined to active ps.sections (active year) to get schoolid/schoolNum.
============================================================================ */

WITH
/* ===== ACTIVE YEAR (same as section.sql) ===== */
active_year AS (
  SELECT MAX(t.yearid) AS active_yearid
  FROM ps.sections sec
  JOIN ps.terms t ON t.id = sec.termid
),

/* ===== COMMON KEYS ===== */
school_key_map AS (
  SELECT id AS school_key, school_number AS schoolNum FROM ps.schools
  UNION ALL SELECT dcid AS school_key, school_number AS schoolNum FROM ps.schools
  UNION ALL SELECT school_number AS school_key, school_number AS schoolNum FROM ps.schools
),

/* ===== ACTIVE SECTIONS (same as section.sql) ===== */
active_secs AS (
  SELECT sec.id, sec.schoolid
  FROM ps.sections sec
  JOIN ps.terms t ON t.id = sec.termid
  WHERE t.yearid = (SELECT active_yearid FROM active_year)
),

/* ===== SCHEDULESECTIONS (AUTHORITATIVE, same approach as section.sql) ===== */
schedsec AS (
  SELECT
    ss.id AS sectionid,
    ss.room
  FROM ps.schedulesections ss
),

/* ===== ROOM SOURCE ===== */
rooms AS (
  SELECT DISTINCT
    CAST(TRIM(ss.room) AS VARCHAR2(20))      AS roomNumber_raw,
    CAST(TO_CHAR(skm.schoolNum) AS VARCHAR2(7)) AS schoolNum_raw
  FROM active_secs a
  JOIN school_key_map skm ON skm.school_key = a.schoolid
  LEFT JOIN schedsec ss   ON ss.sectionid = a.id
  WHERE ss.room IS NOT NULL
    AND LENGTH(TRIM(ss.room)) > 0
    AND skm.schoolNum IS NOT NULL

  UNION ALL

  SELECT DISTINCT
    CAST(TRIM(sec.room) AS VARCHAR2(20))       AS roomNumber_raw,
    CAST(TO_CHAR(skm.schoolNum) AS VARCHAR2(7)) AS schoolNum_raw
  FROM ps.sections sec
  JOIN ps.terms t ON t.id = sec.termid
  JOIN school_key_map skm ON skm.school_key = sec.schoolid
  WHERE t.yearid = (SELECT active_yearid FROM active_year)
    AND sec.room IS NOT NULL
    AND LENGTH(TRIM(sec.room)) > 0
    AND skm.schoolNum IS NOT NULL
),

room_exists AS (
  SELECT COUNT(*) AS cnt FROM rooms
),

ps_source AS (
  SELECT DISTINCT roomNumber_raw, schoolNum_raw
  FROM rooms

  UNION ALL

  SELECT
    CAST('UNKNOWN_FIX_IN_POWERSCHOOL' AS VARCHAR2(20)) AS roomNumber_raw,
    CAST(TO_CHAR(s.school_number) AS VARCHAR2(7))      AS schoolNum_raw
  FROM ps.schools s
  WHERE (SELECT cnt FROM room_exists) = 0
),

/* ===== TRANSFORM ===== */
xform AS (
  SELECT
    SUBSTR(TRIM(roomNumber_raw),1,20) AS roomNumber,
    SUBSTR(TRIM(schoolNum_raw),1,7)   AS schoolNum
  FROM ps_source
),

/* ===== VALIDATE ===== */
validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN roomNumber IS NULL OR TRIM(roomNumber) = '' THEN 'ERR_REQUIRED_roomNumber; ' END ||
      CASE WHEN schoolNum  IS NULL OR TRIM(schoolNum)  = '' THEN 'ERR_REQUIRED_schoolNum; '  END
    ) AS validation_errors
  FROM xform x
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
         '"row_type","roomNumber","schoolNum","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT 1,
         ROW_NUMBER() OVER (ORDER BY schoolNum, roomNumber),

         '"'||
         CASE
           WHEN validation_errors IS NULL OR validation_errors = ''
           THEN 'CLEAN'
           ELSE 'ERROR'
         END||'",'||

         '"'||REPLACE(NVL(roomNumber,''),'"','""')||'",'||
         '"'||REPLACE(NVL(schoolNum,''),'"','""')||'",'||
         '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
  FROM validated
)
ORDER BY sort_order, data_order;