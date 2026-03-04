set heading off
set pagesize 0
set feedback off
set underline off
set newpage none

/* ============================================================================
   ETL: PowerSchool -> Infinite Campus
   Target file: contact.csv
   FIXED:
   - Filters inactive addresses
   - Honors ADDRESSPRIORITYORDER
   - Correct state codeset join
============================================================================ */

WITH

active_students AS (
  SELECT
    st.id   AS studentid,
    st.dcid AS studentdcid,
    st.student_number
  FROM ps.students st
  WHERE st.student_number IS NOT NULL
    AND st.student_number <> 0
    AND st.entrydate <= SYSDATE
    AND (st.exitdate IS NULL OR st.exitdate >= SYSDATE)
),

sca AS (
  SELECT studentcontactassocid,
         studentdcid,
         personid,
         contactpriorityorder,
         currreltypecodesetid
  FROM ps.studentcontactassoc
),

scd AS (
  SELECT studentcontactassocid,
         isactive,
         isemergency,
         iscustodial,
         receivesmailflg,
         liveswithflg
  FROM ps.studentcontactdetail
),

p AS (
  SELECT id AS personid,
         lastname,
         firstname,
         middlename,
         suffixcodesetid,
         gendercodesetid
  FROM ps.person
),

gender_dict AS (
  SELECT codesetid, displayvalue
  FROM ps.codeset
),

relationship_dict AS (
  SELECT codesetid, code
  FROM ps.codeset
),

phones AS (
  SELECT
    personid,
    MAX(CASE WHEN PHONENUMBERPRIORITYORDER = 1 OR ISPREFERRED = 1
             THEN TRIM(PHONENUMBERASENTERED) END) AS primary_phone,
    MAX(CASE WHEN PHONENUMBERPRIORITYORDER = 2
             THEN TRIM(PHONENUMBERASENTERED) END) AS secondary_phone
  FROM ps.personphonenumberassoc
  GROUP BY personid
),

addr AS (
  SELECT
      paa.personid,

      MAX(CASE WHEN paa.addresstypecodesetid = 19 THEN TRIM(pa.street) END) AS phys_street,
      MAX(CASE WHEN paa.addresstypecodesetid = 19 THEN TRIM(pa.city) END)   AS phys_city,
      MAX(CASE WHEN paa.addresstypecodesetid = 19 THEN cs.displayvalue END) AS phys_state,
      MAX(CASE WHEN paa.addresstypecodesetid = 19 THEN TRIM(pa.postalcode) END) AS phys_zip,

      MAX(CASE WHEN paa.addresstypecodesetid = 20 THEN TRIM(pa.street) END) AS mail_street,
      MAX(CASE WHEN paa.addresstypecodesetid = 20 THEN TRIM(pa.city) END)   AS mail_city,
      MAX(CASE WHEN paa.addresstypecodesetid = 20 THEN cs.displayvalue END) AS mail_state,
      MAX(CASE WHEN paa.addresstypecodesetid = 20 THEN TRIM(pa.postalcode) END) AS mail_zip

  FROM ps.personaddressassoc paa
  JOIN ps.personaddress pa
    ON pa.personaddressid = paa.personaddressid
  LEFT JOIN ps.codeset cs
    ON cs.codesetid = pa.statescodesetid
  WHERE paa.enddate IS NULL
    AND paa.addresspriorityorder = 1
  GROUP BY paa.personid
),

ps_source AS (
  SELECT
    CAST(ast.student_number AS VARCHAR2(15)) AS studentNum_raw,
    CAST(p.personid AS VARCHAR2(200)) AS contactKey_raw,
    TRIM(p.lastname)  AS lastName_raw,
    TRIM(p.firstname) AS firstName_raw,
    TRIM(p.middlename) AS middleName_raw,
    TO_CHAR(p.suffixcodesetid) AS suffix_raw,

    CASE
      WHEN UPPER(gd.displayvalue) LIKE 'M%' THEN 'M'
      WHEN UPPER(gd.displayvalue) LIKE 'F%' THEN 'F'
      ELSE NULL
    END AS gender_raw,

    sca.contactpriorityorder AS contactSeq_raw,
    rd.code AS relationship_raw,

    scd.receivesmailflg AS receivesMailFlag_raw,
    scd.liveswithflg    AS livesWithFlag_raw,
    scd.iscustodial     AS isCustodial_raw,
    scd.isemergency     AS emergency_raw,
    scd.isactive        AS isActive_raw,

    CASE WHEN scd.isactive = 1 THEN NULL ELSE SYSDATE END AS endDate_raw,

    a.phys_street AS phys_street_raw,
    a.phys_city   AS phys_city_raw,
    a.phys_state  AS phys_state_raw,
    a.phys_zip    AS phys_zip_raw,

    a.mail_street AS mail_street_raw,
    a.mail_city   AS mail_city_raw,
    a.mail_state  AS mail_state_raw,
    a.mail_zip    AS mail_zip_raw,

    ph.primary_phone,
    ph.secondary_phone

  FROM active_students ast
  JOIN sca ON sca.studentdcid = ast.studentdcid
  JOIN p   ON p.personid = sca.personid
  LEFT JOIN scd  ON scd.studentcontactassocid = sca.studentcontactassocid
  LEFT JOIN addr a ON a.personid = sca.personid
  LEFT JOIN gender_dict gd ON gd.codesetid = p.gendercodesetid
  LEFT JOIN relationship_dict rd ON rd.codesetid = sca.currreltypecodesetid
  LEFT JOIN phones ph ON ph.personid = sca.personid
)

SELECT csv_line
FROM (
  SELECT 0 AS sort_order, 0 AS data_order,
         '"recordType","studentNum","contactKey","lastName","firstName","middleName","suffix","gender","contactSeq","relationship","mailingFlag","livesWithFlag","guardianFlag","emergency","isActive","endDate","householdKey","physicalAddress","physicalCity","physicalState","physicalZip","mailingAddress","mailingCity","mailingState","mailingZip","primaryPhone","secondaryPhone"' AS csv_line
  FROM dual

  UNION ALL

  SELECT 1,
         ROW_NUMBER() OVER (ORDER BY studentNum_raw, contactKey_raw),

         '"CLEAN",'||
         '"'||studentNum_raw||'",'||
         '"'||contactKey_raw||'",'||
         '"'||REPLACE(NVL(lastName_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(firstName_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(middleName_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(suffix_raw,''),'"','""')||'",'||
         '"'||NVL(gender_raw,'')||'",'||
         '"'||NVL(contactSeq_raw,'')||'",'||
         '"'||REPLACE(NVL(relationship_raw,''),'"','""')||'",'||
         '"'||NVL(receivesMailFlag_raw,'')||'",'||
         '"'||NVL(livesWithFlag_raw,'')||'",'||
         '"'||NVL(isCustodial_raw,'')||'",'||
         '"'||NVL(emergency_raw,'')||'",'||
         '"'||NVL(isActive_raw,'')||'",'||
         '"'||NVL(TO_CHAR(endDate_raw,'MM/DD/YYYY'),'')||'",'||
         '"'||REPLACE(NVL(UPPER(mail_street_raw)||'|'||UPPER(mail_city_raw)||'|'||mail_zip_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(phys_street_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(phys_city_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(phys_state_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(phys_zip_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(mail_street_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(mail_city_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(mail_state_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(mail_zip_raw,''),'"','""')||'",'||
         '"'||REPLACE(NVL(primary_phone,''),'"','""')||'",'||
         '"'||REPLACE(NVL(secondary_phone,''),'"','""')||'"'
  FROM ps_source
)
ORDER BY sort_order, data_order;