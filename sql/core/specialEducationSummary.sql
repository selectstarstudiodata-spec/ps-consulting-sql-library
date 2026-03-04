set heading off
set pagesize 0
set feedback off
set underline off
set trimspool on
set linesize 32767
set newpage none

/* ============================================================================
  ETL: PowerSchool > Infinite Campus
  Target file: specialEducationSummary.csv
============================================================================ */

WITH params AS (
  SELECT 2027 AS active_end_year, 3 AS years_of_data FROM dual
),
years AS (
  SELECT (active_end_year - (lvl-1)) AS end_year
  FROM params
  CROSS JOIN (SELECT LEVEL AS lvl FROM dual CONNECT BY LEVEL <= (SELECT years_of_data FROM params))
),
src_counts AS (
  SELECT 
    (SELECT COUNT(*) FROM ps.cst_pubstuspeddata) AS cnt_cst_pubstuspeddata,
    (SELECT COUNT(*) FROM ps.s_nd_stu_x) AS cnt_s_nd_stu_x
  FROM dual
),
has_rows AS (
  SELECT 
    CASE 
      WHEN (SELECT cnt_cst_pubstuspeddata FROM src_counts) > 0 
        OR (SELECT cnt_s_nd_stu_x FROM src_counts) > 0 
      THEN 1 ELSE 0 
    END AS has_sped 
  FROM dual
),

ps_source AS (
  SELECT
    CAST(d.state_studentnumber AS VARCHAR2(15)) AS studentNum_raw,
    d.begindate AS startDate_raw,
    d.enddate AS endDate_raw,
    CAST(
      COALESCE(
        NULLIF(TRIM(d.exceptionalityprimary), ''),
        NULLIF(TRIM(d.primaryorderofdisability), ''),
        NULLIF(TRIM(d.studenteligibility), ''),
        'UNKNOWN'
      ) AS VARCHAR2(50)
    ) AS primaryDisability_raw,
    COALESCE(d.iepbegindate, d.iepreviewdate) AS iepDate_raw,
    d.enddate AS exitDate_raw,
    CAST(NULLIF(TRIM(d.reasonfortermination), '') AS VARCHAR2(50)) AS exitReason_raw,
    CAST(d.schoolyear AS NUMBER(4)) AS endYear_raw
  FROM ps.cst_pubstuspeddata d
  WHERE d.schoolyear IN (SELECT end_year FROM years)
),

nd_extension_source AS (
  SELECT
    CAST(s.state_studentnumber AS VARCHAR2(15)) AS studentNum_raw,
    NULL AS startDate_raw,
    NULL AS endDate_raw,
    CAST(
      COALESCE(
        NULLIF(TRIM(x.servingspecialedunit), ''),
        NULLIF(TRIM(x.sendtotienet), ''),
        'ND_EXT_FLAG'
      ) AS VARCHAR2(50)
    ) AS primaryDisability_raw,
    NULL AS iepDate_raw,
    NULL AS exitDate_raw,
    NULL AS exitReason_raw,
    (SELECT active_end_year FROM params) AS endYear_raw
  FROM ps.students s
  JOIN ps.s_nd_stu_x x ON x.studentsdcid = s.dcid
  WHERE (NULLIF(TRIM(x.servingspecialedunit), '') IS NOT NULL
         OR NULLIF(TRIM(x.sendtotienet), '') IS NOT NULL)
),

combined_source AS (
  SELECT * FROM ps_source
  UNION ALL
  SELECT * FROM nd_extension_source
),

xform AS (
  SELECT
    SUBSTR(TRIM(studentNum_raw),1,15) AS studentNum,
    startDate_raw AS startDate,
    endDate_raw AS endDate,
    SUBSTR(TRIM(primaryDisability_raw),1,50) AS primaryDisability,
    iepDate_raw AS iepDate,
    exitDate_raw AS exitDate,
    SUBSTR(TRIM(exitReason_raw),1,50) AS exitReason,
    endYear_raw AS endYear
  FROM combined_source
),

validated AS (
  SELECT
    x.*,
    TRIM(
      CASE WHEN studentNum IS NULL OR LENGTH(REGEXP_REPLACE(studentNum,'[[:space:]]','')) = 0 THEN 'ERR_REQUIRED_studentNum; ' END ||
      CASE WHEN endYear IS NULL THEN 'ERR_REQUIRED_endYear; ' END
    ) AS validation_errors
  FROM xform x
),

real_rows AS (
  SELECT
    CASE WHEN validation_errors IS NULL OR validation_errors = '' THEN 'CLEAN' ELSE 'ERROR' END AS row_type,
    studentNum,
    startDate,
    endDate,
    primaryDisability,
    iepDate,
    exitDate,
    exitReason,
    endYear,
    validation_errors
  FROM validated
),

info_row AS (
  SELECT
    'INFO' AS row_type,
    CAST(NULL AS VARCHAR2(15)) AS studentNum,
    CAST(NULL AS DATE) AS startDate,
    CAST(NULL AS DATE) AS endDate,
    CAST(NULL AS VARCHAR2(50)) AS primaryDisability,
    CAST(NULL AS DATE) AS iepDate,
    CAST(NULL AS DATE) AS exitDate,
    CAST(NULL AS VARCHAR2(50)) AS exitReason,
    CAST(NULL AS NUMBER(4)) AS endYear,
    'INFO_NO_DATA: CST_PUBSTUSPEDDATA=' ||
      CAST((SELECT cnt_cst_pubstuspeddata FROM src_counts) AS VARCHAR2(20)) ||
      ', S_ND_STU_X=' ||
      CAST((SELECT cnt_s_nd_stu_x FROM src_counts) AS VARCHAR2(20)) ||
      '; no rows available to export for requested years.' AS validation_errors
  FROM dual
  WHERE (SELECT has_sped FROM has_rows) = 0
),

final_data AS (
  SELECT * FROM real_rows
  UNION ALL
  SELECT * FROM info_row
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
    '"row_type","studentNum","startDate","endDate","primaryDisability","iepDate","exitDate","exitReason","endYear","validation_errors"' AS csv_line
  FROM dual

  UNION ALL

  SELECT
    1,
    ROW_NUMBER() OVER (ORDER BY studentNum, startDate),

    '"'||REPLACE(NVL(row_type,''),'"','""')||'",'||
    '"'||REPLACE(NVL(studentNum,''),'"','""')||'",'||
    '"'||NVL(TO_CHAR(startDate,'MM/DD/YYYY'),'')||'",'||
    '"'||NVL(TO_CHAR(endDate,'MM/DD/YYYY'),'')||'",'||
    '"'||REPLACE(NVL(primaryDisability,''),'"','""')||'",'||
    '"'||NVL(TO_CHAR(iepDate,'MM/DD/YYYY'),'')||'",'||
    '"'||NVL(TO_CHAR(exitDate,'MM/DD/YYYY'),'')||'",'||
    '"'||REPLACE(NVL(exitReason,''),'"','""')||'",'||
    '"'||NVL(TO_CHAR(endYear),'')||'",'||
    '"'||REPLACE(NVL(validation_errors,''),'"','""')||'"'

  FROM final_data
)
ORDER BY sort_order, data_order;