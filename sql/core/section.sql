/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: section.csv
   Key = SECTION_NUMBER (matches roster.csv)
   No dependency on SCHEDULESECTIONS table
============================================================================ */

WITH
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
    t.schoolid
  FROM ps.terms t
  JOIN school_key_map skm
    ON skm.school_key = t.schoolid
  WHERE t.isyearrec = 1
    AND t.yearid = (SELECT active_yearid FROM active_year)
    AND skm.schoolNum <> 999999
),

calendar_base AS (
  SELECT
    schoolNum,
    schoolName,
    firstday,
    lastday,
    CASE
      WHEN EXTRACT(MONTH FROM firstday) IN (6,7)
      THEN SUBSTR(EXTRACT(YEAR FROM firstday),-2)||'-'||SUBSTR(EXTRACT(YEAR FROM lastday),-2)||' Summer School'
      ELSE SUBSTR(EXTRACT(YEAR FROM firstday),-2)||'-'||SUBSTR(EXTRACT(YEAR FROM lastday),-2)||' '||SUBSTR(TRIM(schoolName),1,23)
    END calendarName_base,
    schoolid
  FROM yearrec_terms
),

calendar_named AS (
  SELECT c.*, COUNT(*) OVER (PARTITION BY calendarName_base) name_ct
  FROM calendar_base c
),

calendar_final AS (
  SELECT
    schoolNum,
    schoolid,
    CASE
      WHEN name_ct=1 THEN SUBSTR(calendarName_base,1,30)
      ELSE SUBSTR(calendarName_base,1,30-(1+LENGTH(schoolNum)))||'-'||schoolNum
    END calendarName
  FROM calendar_named
),

active_secs AS (
  SELECT DISTINCT
    sec.section_number,
    sec.course_number,
    sec.schoolid,
    sec.teacher,
    sec.room,
    sec.maxenrollment,
    sec.team
  FROM ps.sections sec
  JOIN ps.terms t
    ON t.id = sec.termid
   AND t.schoolid = sec.schoolid
  WHERE t.yearid = (SELECT active_yearid FROM active_year)
),

ps_source AS (
  SELECT DISTINCT
    TO_CHAR(a.section_number) AS sectionNumber_raw,
    TRIM(a.course_number)     AS courseNumber_raw,
    skm.schoolNum             AS schoolNum_raw,
    cf.calendarName           AS calendarName_raw,
    a.room                    AS roomNumber_raw,
    u.teachernumber           AS teacherNumber_raw,
    a.maxenrollment           AS maxEnrollment_raw,
    a.team                    AS team_raw,
    CAST(NULL AS NUMBER(1))   AS exclude_raw
  FROM active_secs a
  JOIN school_key_map skm
    ON skm.school_key = a.schoolid
  JOIN calendar_final cf
    ON cf.schoolid = a.schoolid
  LEFT JOIN ps.schoolstaff ss
    ON ss.users_dcid = a.teacher
   AND ss.schoolid   = a.schoolid
  LEFT JOIN ps.users u
    ON u.dcid = ss.users_dcid
),

xform AS (
  SELECT
    SUBSTR(TRIM(sectionNumber_raw),1,20) AS sectionNumber,
    SUBSTR(TRIM(courseNumber_raw),1,20)  AS courseNumber,
    SUBSTR(TRIM(schoolNum_raw),1,7)      AS schoolNum,
    SUBSTR(TRIM(calendarName_raw),1,30)  AS calendarName,
    SUBSTR(TRIM(roomNumber_raw),1,20)    AS roomNumber,
    teacherNumber_raw                    AS primaryEmployNum,
    maxEnrollment_raw                    AS maxEnrollment,
    SUBSTR(TRIM(team_raw),1,20)          AS team,
    exclude_raw                          AS exclude
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN sectionNumber IS NULL OR TRIM(sectionNumber)='' THEN 'ERR_REQUIRED_sectionNumber; ' END ||
      CASE WHEN courseNumber IS NULL  OR TRIM(courseNumber)=''  THEN 'ERR_REQUIRED_courseNumber; '  END ||
      CASE WHEN schoolNum IS NULL     OR TRIM(schoolNum)=''     THEN 'ERR_REQUIRED_schoolNum; '     END ||
      CASE WHEN calendarName IS NULL  OR TRIM(calendarName)=''  THEN 'ERR_REQUIRED_calendarName; '  END
    ) validation_errors
  FROM xform x
),

final_data AS (
  SELECT
    CASE WHEN validation_errors IS NULL OR validation_errors='' THEN 'CLEAN' ELSE 'ERROR' END row_type,
    sectionNumber,
    courseNumber,
    schoolNum,
    calendarName,
    roomNumber,
    primaryEmployNum,
    maxEnrollment,
    team,
    exclude,
    validation_errors
  FROM validated
)

SELECT
  '"row_type","sectionNum","courseNum","schoolNum","calendarName","roomNumber","primaryEmployNum","maxStudents","teamName","exclude","validation_errors"'
FROM dual

UNION ALL

SELECT
  '"'||row_type||'",'||
  '"'||sectionNumber||'",'||
  '"'||courseNumber||'",'||
  '"'||schoolNum||'",'||
  '"'||REPLACE(NVL(calendarName,''),'"','""')||'",'||
  '"'||NVL(roomNumber,'')||'",'||
  '"'||NVL(primaryEmployNum,'')||'",'||
  '"'||NVL(maxEnrollment,'')||'",'||
  '"'||NVL(team,'')||'",'||
  '"'||NVL(exclude,'')||'",'||
  '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'
FROM final_data;