# Running Extracts (Portfolio)

This repo contains representative SQL written to be readable and portable.
Table/field names are generalized.

## Parameters used in this repo

Most extracts accept:

- `:as_of_date` (DATE)
  - The effective date used to determine “currently active” enrollment.
- `:school_id` (INT, optional)
  - If NULL, returns all schools. If provided, filters to one school.

## Example: Student Roster Extract

File:
- `sql/extracts/student_roster_extract.sql`

Expected output (typical columns):
- student_id
- state_id, local_id
- first_name, last_name
- grade_level, school_id
- enroll_date, exit_date

### Example parameter values
- `:as_of_date = '2026-08-15'`
- `:school_id = NULL` (all schools)
  - or `:school_id = 101` (single school)

## Notes on portability

- In a real district environment, map the generalized tables to local names:
  - `students`
  - `student_identifiers`
  - `student_demographics`
- Keep client credentials and connection strings out of this repo.
- Do not commit output files (CSV/XLSX) to a public repository.
