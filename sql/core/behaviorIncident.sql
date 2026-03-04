set heading off
set feedback off
set pagesize 0
set verify off
set trimspool on
set underline off
set newpage none

/* ============================================================================
   ETL: PowerSchool > Infinite Campus
   Target file: behaviorIncident.csv
============================================================================ */

WITH
params AS (
  SELECT DATE '2019-07-01' AS win_start,
         DATE '2027-06-30' AS win_end
  FROM dual
),

source_flags AS (
  SELECT
    (SELECT COUNT(*) FROM ps.log)      AS log_rows,
    (SELECT COUNT(*) FROM ps.incident) AS incident_rows
  FROM dual
),

log_source AS (
  SELECT
    CAST(li.id AS VARCHAR2(50)) AS incidentID_raw,
    CAST(li.schoolid AS NUMBER) AS schoolID_raw,
    CAST(COALESCE(li.discipline_incidentdate, li.entry_date) AS DATE) AS incidentDate_raw,
    CAST(li.entry_time AS VARCHAR2(20)) AS incidentTime_raw,
    CAST(COALESCE(li.discipline_incidentlocation, li.discipline_incidentlocdetail) AS VARCHAR2(100)) AS location_raw,
    CAST(
      COALESCE(
        NULLIF(TRIM(li.subject), ''),
        NULLIF(TRIM(DBMS_LOB.SUBSTR(li.entry, 2000, 1)), ''),
        NULLIF(TRIM(DBMS_LOB.SUBSTR(li.custom, 2000, 1)), '')
      ) AS VARCHAR2(2000)
    ) AS entryText_raw,
    CAST(li.logtypeid AS NUMBER) AS logTypeID_raw,
    CAST(NULL AS TIMESTAMP(6)) AS whenCreated_raw,
    CAST(NULL AS TIMESTAMP(6)) AS whenModified_raw
  FROM ps.log li
  WHERE
    (SELECT log_rows FROM source_flags) > 0
    AND TRUNC(CAST(COALESCE(li.discipline_incidentdate, li.entry_date) AS DATE)) 
        BETWEEN (SELECT win_start FROM params)
            AND (SELECT win_end FROM params)
),

incident_source AS (
  SELECT
    CAST(i.incident_id AS VARCHAR2(50)) AS incidentID_raw,
    CAST(i.school_number AS NUMBER) AS schoolID_raw,
    CAST(i.incident_ts AS DATE) AS incidentDate_raw,
    CAST(TO_CHAR(i.incident_ts,'HH24:MI:SS') AS VARCHAR2(20)) AS incidentTime_raw,
    CAST(i.location_details AS VARCHAR2(100)) AS location_raw,
    CAST(NULLIF(TRIM(DBMS_LOB.SUBSTR(i.incident_detail_desc,2000,1)), '') AS VARCHAR2(2000)) AS entryText_raw,
    CAST(NULL AS NUMBER) AS logTypeID_raw,
    CAST(i.created_ts AS TIMESTAMP(6)) AS whenCreated_raw,
    CAST(i.last_modified_ts AS TIMESTAMP(6)) AS whenModified_raw
  FROM ps.incident i
  WHERE
    (SELECT incident_rows FROM source_flags) > 0
    AND TRUNC(CAST(i.incident_ts AS DATE))
        BETWEEN (SELECT win_start FROM params)
            AND (SELECT win_end FROM params)
),

ps_source AS (
  SELECT * FROM log_source
  UNION ALL
  SELECT * FROM incident_source
),

xform AS (
  SELECT
    SUBSTR(TRIM(incidentID_raw),1,50) AS incidentID,
    CAST(schoolID_raw AS NUMBER) AS schoolID,
    CAST(incidentDate_raw AS DATE) AS incidentDate,
    SUBSTR(TRIM(incidentTime_raw),1,20) AS incidentTime,
    SUBSTR(TRIM(location_raw),1,100) AS location,
    SUBSTR(TRIM(entryText_raw),1,2000) AS entryText,
    CAST(logTypeID_raw AS NUMBER) AS logTypeID,
    CAST(whenCreated_raw AS TIMESTAMP(6)) AS whenCreated,
    CAST(whenModified_raw AS TIMESTAMP(6)) AS whenModified
  FROM ps_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN incidentID IS NULL OR TRIM(incidentID)='' THEN 'ERR_REQUIRED_incidentID; ' END ||
      CASE WHEN incidentDate IS NULL THEN 'ERR_REQUIRED_incidentDate; ' END
    ) AS validation_errors,
    CASE WHEN incidentDate IS NULL THEN 'NO_INCIDENT_DATE' END AS reviewReason
  FROM xform x
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
  '"row_type","incidentID","schoolID","incidentDate","incidentTime","location","entryText","logTypeID","whenCreated","whenModified","reviewReason","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    1,
    ROW_NUMBER() OVER (ORDER BY incidentID),

    '"'||
    CASE
      WHEN validation_errors IS NULL OR validation_errors='' THEN
        CASE WHEN reviewReason IS NULL THEN 'CLEAN' ELSE 'ERROR' END
      ELSE 'ERROR'
    END||'",'||

    '"'||REPLACE(NVL(incidentID,''),'"','""')||'",'||

    '"'||NVL(TO_CHAR(schoolID),'')||'",'||

    '"'||NVL(TO_CHAR(incidentDate,'MM/DD/YYYY'),'')||'",'||

    '"'||REPLACE(NVL(incidentTime,''),'"','""')||'",'||

    '"'||REPLACE(NVL(location,''),'"','""')||'",'||

    '"'||
      REPLACE(
        REPLACE(
          REPLACE(NVL(entryText,''), CHR(13), ' '),
        CHR(10), ' '),
      '"','""'
      )
    ||'",'||

    '"'||NVL(TO_CHAR(logTypeID),'')||'",'||

    '"'||NVL(TO_CHAR(whenCreated),'')||'",'||

    '"'||NVL(TO_CHAR(whenModified),'')||'",'||

    '"'||REPLACE(NVL(reviewReason,''),'"','""')||'",'||

    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'

  FROM validated
)

ORDER BY sort_order, data_order;