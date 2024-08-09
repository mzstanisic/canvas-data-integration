# TODO

- data_integrator.join_dataframes needs way more data to match. Start pulling json
files for Summer 2024. Also, try left joins for canvas_full files
- requirements.txt: <https://pip.pypa.io/en/stable/cli/pip_freeze/#pip-freeze>
- rotate logs past retention period
- check if records already exist in tables before insertion (most recent timestamp in table, don't insert anything before that)
- redo all module and function docstrings
- last_seen implementation in config/canvas_extractor/database_uploader

## SETUP

Optionally, set up a virtual environment to not pollute the global one.
--setup virtual environment

python -m pip install -r requirements.txt

Create tables in Oracle Database. Oracle scripts derived and condensed from DAP API scripts for [PostgreSQL](https://data-access-platform-api.s3.eu-central-1.amazonaws.com/sql/postgresql.sql), [MySQL](https://data-access-platform-api.s3.eu-central-1.amazonaws.com/sql/mysql.sql), [Microsoft SQL Server](https://data-access-platform-api.s3.amazonaws.com/sql/mssql.sql).

### canvas_course_sections

```sql
CREATE TABLE canvas_course_sections (
    course_sections_id NUMBER(19) NOT NULL,
    course_sections_name VARCHAR2(255) NOT NULL,
    course_sections_course_id NUMBER(19) NOT NULL,
    course_sections_workflow_state VARCHAR2(255) NOT NULL,
    course_sections_ts TIMESTAMP NOT NULL,
    CONSTRAINT pk_course_sections PRIMARY KEY (course_sections_id)
    );
```

### canvas_courses

```sql
CREATE TABLE canvas_courses (
    courses_id NUMBER(19) NOT NULL,
    courses_sis_source_id VARCHAR2(255),
    courses_name VARCHAR2(255),
    courses_enrollment_term_id NUMBER(19) NOT NULL,
    courses_workflow_state VARCHAR2(255) NOT NULL,
    courses_is_public VARCHAR2(5),
    courses_ts TIMESTAMP NOT NULL,
    CONSTRAINT pk_courses PRIMARY KEY (courses_id)
    );
```

### canvas_enrollment_terms

```sql
CREATE TABLE canvas_enrollment_terms (
    enrollment_terms_id NUMBER(19) NOT NULL,
    enrollment_terms_sis_source_id VARCHAR2(255),
    enrollment_terms_workflow_state VARCHAR2(255) NOT NULL,
    enrollment_terms_ts TIMESTAMP NOT NULL,
    CONSTRAINT pk_enrollment_terms PRIMARY KEY (enrollment_terms_id)
    );
```  

### canvas_enrollments

```sql
CREATE TABLE canvas_enrollments (
    enrollments_id NUMBER(19) NOT NULL,
    enrollments_last_activity_at TIMESTAMP,
    enrollments_total_activity_time NUMBER(10),
    enrollments_course_section_id NUMBER(19) NOT NULL,
    enrollments_course_id NUMBER(19) NOT NULL,
    enrollments_role_id NUMBER(19) NOT NULL,
    enrollments_user_id NUMBER(19) NOT NULL,
    enrollments_sis_pseudonym_id NUMBER(19),
    enrollments_workflow_state VARCHAR2(255) NOT NULL,
    enrollments_type VARCHAR2(255) NOT NULL,
    enrollments_ts TIMESTAMP NOT NULL,
    CONSTRAINT pk_enrollments PRIMARY KEY (enrollments_id)
    );
```

### canvas_pseudonyms

```sql
CREATE TABLE canvas_pseudonyms (
    pseudonyms_id NUMBER(19) NOT NULL,
    pseudonyms_user_id NUMBER(19) NOT NULL,
    pseudonyms_workflow_state VARCHAR2(255) NOT NULL,
    pseudonyms_unique_id VARCHAR2(255) NOT NULL,
    pseudonyms_sis_user_id VARCHAR2(255),
    pseudonyms_ts TIMESTAMP NOT NULL,
    CONSTRAINT pk_pseudonyms PRIMARY KEY (pseudonyms_id)
    );
```

### canvas_scores

```sql
CREATE TABLE canvas_scores (
    scores_id NUMBER(19) NOT NULL,
    scores_current_score BINARY_DOUBLE,
    scores_enrollment_id NUMBER(19) NOT NULL,
    scores_workflow_state VARCHAR2(255) NOT NULL,
    scores_course_score VARCHAR2(5) NOT NULL,
    scores_ts TIMESTAMP NOT NULL,
    CONSTRAINT pk_scores PRIMARY KEY (scores_id)
    );
```

### canvas_users

```sql
CREATE TABLE canvas_users (
    users_id NUMBER(19) NOT NULL,
    users_workflow_state VARCHAR2(255) NOT NULL,
    users_name VARCHAR2(255),
    users_ts TIMESTAMP NOT NULL,
    CONSTRAINT pk_users PRIMARY KEY (users_id)
    );
```

## Resources

https://community.canvaslms.com/t5/Data-and-Analytics-Group/Generating-SQL-Create-Table-Scripts-from-JSON-Schemas-for-Canvas/m-p/588305