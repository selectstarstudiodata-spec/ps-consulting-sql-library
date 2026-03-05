whenever sqlerror continue

/* ===== GLOBAL DEFAULTS ===== */
set echo on
set verify off
set feedback on
set heading on
set pagesize 0
set linesize 32767
set trimspool on
set tab off
set sqlblanklines on
set colsep ','
set termout on
set timing on
set underline off

prompt === START ALL BATCHES ===

spool "E:\etl_scaffold\Output\District_Data_Quality\Alexander\MASTER_runlog.txt" append
prompt ===== RUN START =====
select to_char(systimestamp,'YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM') start_ts from dual;
prompt
spool off


/* =========================================================
   ======================== BATCH 1 ========================
   ========================================================= */

define OUT_BASE=E:\etl_scaffold\Output\District_Data_Quality\Alexander\Batch_1
define SQL_BASE=E:\etl_scaffold\sql\Alexander\Batch_1

/* CSV mode */
set echo off
set termout off
set feedback off
set timing off
set verify off
set sqlprompt ''
set heading off
set pagesize 0
set linesize 32767
set trimspool on
set tab off
set underline off
set colsep off

/* Schools & Calendars */
spool "&&OUT_BASE\school.csv" replace
@@ "&&SQL_BASE.\Schools+Calendars\school_Alexander.sql"
spool off

spool "&&OUT_BASE\calendar.csv" replace
@@ "&&SQL_BASE.\Schools+Calendars\calendar_Alexander.sql"
spool off

spool "&&OUT_BASE\gradeLevel.csv" replace
@@ "&&SQL_BASE.\Schools+Calendars\gradeLevel_Alexander.sql"
spool off

spool "&&OUT_BASE\term.csv" replace
@@ "&&SQL_BASE.\Schools+Calendars\term_Alexander.sql"
spool off

spool "&&OUT_BASE\periodSchedule.csv" replace
@@ "&&SQL_BASE.\Schools+Calendars\periodSchedule_Alexander.sql"
spool off

spool "&&OUT_BASE\period.csv" replace
@@ "&&SQL_BASE.\Schools+Calendars\period_Alexander.sql"
spool off

spool "&&OUT_BASE\department.csv" replace
@@ "&&SQL_BASE.\Schools+Calendars\department_Alexander.sql"
spool off

spool "&&OUT_BASE\room.csv" replace
@@ "&&SQL_BASE.\Schools+Calendars\room_Alexander.sql"
spool off

/* Schedules */
spool "&&OUT_BASE\course.csv" replace
@@ "&&SQL_BASE.\Schedules\course_Alexander.sql"
spool off

spool "&&OUT_BASE\section.csv" replace
@@ "&&SQL_BASE.\Schedules\section_Alexander.sql"
spool off

spool "&&OUT_BASE\sectionSchedule.csv" replace
@@ "&&SQL_BASE.\Schedules\sectionSchedule_Alexander.sql"
spool off

spool "&&OUT_BASE\roster.csv" replace
@@ "&&SQL_BASE.\Schedules\roster_Alexander.sql"
spool off

spool "&&OUT_BASE\courseRequest.csv" replace
@@ "&&SQL_BASE.\Schedules\courseRequest_Alexander.sql"
spool off

/* People */
spool "&&OUT_BASE\student.csv" replace
@@ "&&SQL_BASE.\People\student_Alexander.sql"
spool off

spool "&&OUT_BASE\contact.csv" replace
@@ "&&SQL_BASE.\People\contact_Alexander.sql"
spool off


/* =========================================================
   ======================== BATCH 2 ========================
   ========================================================= */

define OUT_BASE=E:\etl_scaffold\Output\District_Data_Quality\Alexander\Batch_2
define SQL_BASE=E:\etl_scaffold\sql\Alexander\Batch_2

set heading on
set pagesize 50000
set colsep ','

/* Behavior */
spool "&&OUT_BASE\behaviorEventType.csv" replace
@@ "&&SQL_BASE\Behavior\behaviorEventType_Alexander.sql"
spool off

spool "&&OUT_BASE\behaviorIncident.csv" replace
@@ "&&SQL_BASE\Behavior\behaviorIncident_Alexander.sql"
spool off

spool "&&OUT_BASE\behaviorResolution.csv" replace
@@ "&&SQL_BASE\Behavior\behaviorResolution_Alexander.sql"
spool off

spool "&&OUT_BASE\behaviorResolutionType.csv" replace
@@ "&&SQL_BASE\Behavior\behaviorResolutionType_Alexander.sql"
spool off

spool "&&OUT_BASE\behaviorRole.csv" replace
@@ "&&SQL_BASE\Behavior\behaviorRole_Alexander.sql"
spool off

/* Grades */
spool "&&OUT_BASE\transcript.csv" replace
@@ "&&SQL_BASE\Grades\transcript_Alexander.sql"
spool off

/* Grading Setup */
spool "&&OUT_BASE\gradingTask.csv" replace
@@ "&&SQL_BASE\Grading_Setup\gradingTask_Alexander.sql"
spool off

spool "&&OUT_BASE\gradingComment.csv" replace
@@ "&&SQL_BASE\Grading_Setup\gradingComment_Alexander.sql"
spool off

spool "&&OUT_BASE\standard.csv" replace
@@ "&&SQL_BASE\Grading_Setup\standards_Alexander.sql"
spool off

spool "&&OUT_BASE\standard.csv" replace
@@ "&&SQL_BASE\Grading_Setup\standards_Alexander.sql"
spool off

spool "&&OUT_BASE\studentstandardscore.csv" replace
@@ "&&SQL_BASE\Grading_Setup\studentstandardscore_Alexander.sql"
spool off

/* Health */
spool "&&OUT_BASE\healthCondition.csv" replace
@@ "&&SQL_BASE\Health\healthCondition_Alexander.sql"
spool off

/* People */
spool "&&OUT_BASE\employee.csv" replace
@@ "&&SQL_BASE\People\employee_Alexander.sql"
spool off

/* Security */
spool "&&OUT_BASE\userAccount.csv" replace
@@ "&&SQL_BASE\Security\userAccount_Alexander.sql"
spool off

/* Students */
spool "&&OUT_BASE\enrollment.csv" replace
@@ "&&SQL_BASE\Students\enrollment_Alexander.sql"
spool off

spool "&&OUT_BASE\specialEducationSummary.csv" replace
@@ "&&SQL_BASE\Students\specialEducationSummary_Alexander.sql"
spool off

spool "&&OUT_BASE\2627Enrollment.csv" replace
@@ "&&SQL_BASE\Students\2627Enrollment_Alexander.sql"
spool off


/* =========================================================
   ======================== BATCH 3 ========================
   ========================================================= */

define OUT_BASE=E:\etl_scaffold\Output\District_Data_Quality\Alexander\Batch_3
define SQL_BASE=E:\etl_scaffold\sql\Alexander\Batch_3

spool "&&OUT_BASE\freeReducedEligibility.csv" replace
@@ "&&SQL_BASE\Students\freeReducedEligibility_Alexander.sql"
spool off

spool "&&OUT_BASE\districtResidency.csv" replace
@@ "&&SQL_BASE\Students\districtResidency_Alexander.sql"
spool off

spool "&&OUT_BASE\gifted.csv" replace
@@ "&&SQL_BASE\Students\gifted_Alexander.sql"
spool off

spool "&&OUT_BASE\cte.csv" replace
@@ "&&SQL_BASE\Students\cte_Alexander.sql"
spool off

spool "&&OUT_BASE\Iep.csv" replace
@@ "&&SQL_BASE\Students\Iep_Alexander.sql"
spool off

spool "&&OUT_BASE\IepServiceND.csv" replace
@@ "&&SQL_BASE\Students\IepServiceND_Alexander.sql"
spool off

spool "&&OUT_BASE\earlyLearning.csv" replace
@@ "&&SQL_BASE\Students\earlyLearning_Alexander.sql"
spool off

spool "&&OUT_BASE\fosterCare.csv" replace
@@ "&&SQL_BASE\Students\fosterCare_Alexander.sql"
spool off

spool "&&OUT_BASE\homeSchool.csv" replace
@@ "&&SQL_BASE\Students\homeSchool_Alexander.sql"
spool off

spool "&&OUT_BASE\homeless.csv" replace
@@ "&&SQL_BASE\Students\homeless_Alexander.sql"
spool off

spool "&&OUT_BASE\homelessService.csv" replace
@@ "&&SQL_BASE\Students\homelessService_Alexander.sql"
spool off

spool "&&OUT_BASE\migrant.csv" replace
@@ "&&SQL_BASE\Students\migrant_Alexander.sql"
spool off

spool "&&OUT_BASE\militaryConnections.csv" replace
@@ "&&SQL_BASE\Students\militaryConnections_Alexander.sql"
spool off

spool "&&OUT_BASE\pregnantAndParenting.csv" replace
@@ "&&SQL_BASE\Students\pregnantAndParenting_Alexander.sql"
spool off

spool "&&OUT_BASE\section504.csv" replace
@@ "&&SQL_BASE\Students\section504_Alexander.sql"
spool off

spool "&&OUT_BASE\SAAP.csv" replace
@@ "&&SQL_BASE\Students\SAAP_Alexander.sql"
spool off

spool "&&OUT_BASE\twentyFirstCCLC.csv" replace
@@ "&&SQL_BASE\Students\twentyFirstCCLC_Alexander.sql"
spool off

spool "&&OUT_BASE\title1PartA.csv" replace
@@ "&&SQL_BASE\Students\title1PartA_Alexander.sql"
spool off

spool "&&OUT_BASE\title1PartD.csv" replace
@@ "&&SQL_BASE\Students\title1PartD_Alexander.sql"
spool off

spool "&&OUT_BASE\title3.csv" replace
@@ "&&SQL_BASE\Students\title3_Alexander.sql"
spool off

spool "&&OUT_BASE\flag.csv" replace
@@ "&&SQL_BASE\Flags\flag_Alexander.sql"
spool off

spool "&&OUT_BASE\flagParticipation.csv" replace
@@ "&&SQL_BASE\Flags\flagParticipation_Alexander.sql"
spool off

spool "&&OUT_BASE\employment.csv" replace
@@ "&&SQL_BASE\Employees\employment_Alexander.sql"
spool off


/* =========================================================
   ======================== BATCH 4 ========================
   ========================================================= */

define OUT_BASE=E:\etl_scaffold\Output\District_Data_Quality\Alexander\Batch_4
define SQL_BASE=E:\etl_scaffold\sql\Alexander\Batch_4

spool "&&OUT_BASE\healthVisit.csv" replace
@@ "&&SQL_BASE\Health\healthVisit_Alexander.sql"
spool off

spool "&&OUT_BASE\childAndTeenScreening.csv" replace
@@ "&&SQL_BASE\Screenings\childAndTeenScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\concussionScreening.csv" replace
@@ "&&SQL_BASE\Screenings\concussionScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\dentalScreening.csv" replace
@@ "&&SQL_BASE\Screenings\dentalScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\developmentalScreening.csv" replace
@@ "&&SQL_BASE\Screenings\developmentalScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\earlyChildhoodScreening.csv" replace
@@ "&&SQL_BASE\Screenings\earlyChildhoodScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\hearingScreening.csv" replace
@@ "&&SQL_BASE\Screenings\hearingScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\healthWeightScreening.csv" replace
@@ "&&SQL_BASE\Screenings\healthWeightScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\leadScreening.csv" replace
@@ "&&SQL_BASE\Screenings\leadScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\scoliosisScreening.csv" replace
@@ "&&SQL_BASE\Screenings\scoliosisScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\socialEmotionalScreening.csv" replace
@@ "&&SQL_BASE\Screenings\socialEmotionalScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\speechScreening.csv" replace
@@ "&&SQL_BASE\Screenings\speechScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\sportsScreening.csv" replace
@@ "&&SQL_BASE\Screenings\sportsScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\tuberculosisScreening.csv" replace
@@ "&&SQL_BASE\Screenings\tuberculosisScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\visionScreening.csv" replace
@@ "&&SQL_BASE\Screenings\visionScreening_Alexander.sql"
spool off

spool "&&OUT_BASE\locker.csv" replace
@@ "&&SQL_BASE\Lockers\locker_Alexander.sql"
spool off

spool "&&OUT_BASE\fee.csv" replace
@@ "&&SQL_BASE\Fees\fee_Alexander.sql"
spool off

spool "&&OUT_BASE\courseFee.csv" replace
@@ "&&SQL_BASE\Fees\courseFee_Alexander.sql"
spool off

spool "&&OUT_BASE\attendanceAggregate.csv" replace
@@ "&&SQL_BASE\misc\attendanceAggregate_Alexander.sql"
spool off


/* ========================================================= */

set echo on
set termout on
set feedback on
set timing on
set heading on
set sqlprompt 'SQL> '

prompt === END ALL BATCHES ===

spool "E:\etl_scaffold\Output\District_Data_Quality\Alexander\MASTER_runlog.txt" append
select to_char(systimestamp,'YYYY-MM-DD HH24:MI:SS.FF3 TZH:TZM') end_ts from dual;
prompt ===== RUN END =====
spool off

exit

