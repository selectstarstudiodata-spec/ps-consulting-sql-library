set heading off
set pagesize 0
set feedback off
set underline off
set newpage none

WITH school_key_map AS (
  SELECT id AS school_key, school_number AS schoolNum, name AS schoolName, NVL(IsSummerSchool,0) AS isSummerSchool
  FROM ps.schools
  UNION ALL
  SELECT dcid, school_number, name, NVL(IsSummerSchool,0)
  FROM ps.schools
  UNION ALL
  SELECT school_number, school_number, name, NVL(IsSummerSchool,0)
  FROM ps.schools
),

yearrec_terms AS (
  SELECT
      skm.schoolNum,
      skm.schoolName,
      skm.isSummerSchool,
      t.firstday AS startDate,
      t.lastday  AS endDate,
      FLOOR(t.id/100) AS year_index
  FROM ps.terms t
  JOIN school_key_map skm
    ON skm.school_key = t.schoolid
  WHERE t.isyearrec = 1
    AND skm.schoolNum IS NOT NULL
    AND skm.schoolNum <> 999999
),

xform AS (
  SELECT
      SUBSTR(TRIM(schoolNum),1,7) AS schoolNum,

      SUBSTR(TO_CHAR(EXTRACT(YEAR FROM startDate)), -2) || '-' ||
      SUBSTR(TO_CHAR(EXTRACT(YEAR FROM startDate) + 1), -2) || ' ' ||
      SUBSTR(REGEXP_REPLACE(TRIM(schoolName), '\s+[0-9]+$', ''),1,23) AS calendarName,

      startDate,
      endDate,
      year_index AS calendarNumber,
      1 AS sequence,
      EXTRACT(YEAR FROM startDate) + 1 AS schoolYear,
      isSummerSchool
  FROM yearrec_terms
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN schoolNum IS NULL THEN 'ERR_REQUIRED_schoolNum; ' END ||
      CASE WHEN calendarName IS NULL THEN 'ERR_REQUIRED_calendarName; ' END ||
      CASE WHEN startDate IS NULL THEN 'ERR_REQUIRED_startDate; ' END ||
      CASE WHEN endDate IS NULL THEN 'ERR_REQUIRED_endDate; ' END ||
      CASE WHEN startDate > endDate THEN 'ERR_INVALID_dateRange; ' END
    ) AS validation_errors
  FROM xform x
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
         '"row_type","schoolNum","calendarName","startDate","endDate","calendarNumber","sequence","schoolYear","summerSchool","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT 1,
         ROW_NUMBER() OVER (ORDER BY schoolNum, startDate),

         '"'||
         CASE
           WHEN validation_errors IS NULL OR validation_errors = ''
           THEN 'CLEAN'
           ELSE 'ERROR'
         END||'",'||

         '"'||REPLACE(NVL(schoolNum,''),'"','""')||'",'||
         '"'||REPLACE(NVL(calendarName,''),'"','""')||'",'||
         '"'||NVL(TO_CHAR(startDate,'YYYY-MM-DD'),'')||'",'||
         '"'||NVL(TO_CHAR(endDate,'YYYY-MM-DD'),'')||'",'||
         '"'||NVL(TO_CHAR(calendarNumber),'')||'",'||
         '"'||NVL(TO_CHAR(sequence),'')||'",'||
         '"'||NVL(TO_CHAR(schoolYear),'')||'",'||
         '"'||NVL(TO_CHAR(isSummerSchool),'')||'",'||
         '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
  FROM validated
)
ORDER BY sort_order, data_order;