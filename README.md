# TODO

- data_integrator.join_dataframes needs way more data to match. Start pulling json
files for Summer 2024. Also, try left joins for canvas_full files
- requirements.txt: <https://pip.pypa.io/en/stable/cli/pip_freeze/#pip-freeze>
- rotate logs past retention period
- check for relevant environment variables, otherwise search for file
- change defaults from CSV to JSON

## SETUP

Create tables in Oracle Database:

### canvas_course_sections

### canvas_courses

### canvas_enrollment_terms

```sql
CREATE TABLE canvas_enrollment_terms (
    enrollment_terms_id NUMBER(4) NOT NULL,
    enrollment_terms_sis_source_id VARCHAR2(128),
    enrollment_terms_workflow_state VARCHAR2(26),
    enrollment_terms_ts TIMESTAMP
    )
```  

### canvas_enrollments

### canvas_pseudonyms

### canvas_scores

### canvas_users
