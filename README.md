# CANVAS DATA INTEGRATION

This application uses Instructure's [Data Access Platform (DAP)](https://data-access-platform-api.s3.amazonaws.com/index.html) to pull Canvas table data into data files that we can then insert into our database to perform operations and reporting on. Since DAP does not yet natively support Oracle for [replicating](https://data-access-platform-api.s3.amazonaws.com/client/README.html#replicating-data-to-a-database) and [synchronizing](https://data-access-platform-api.s3.amazonaws.com/client/README.html#synchronizing-data-with-a-database) data to a database (the preferred solution), we developed this application to support data replication to Oracle with a desired subset of data for further operations and reporting. The end result is a setup of Oracle tables and views that provide data to stakeholders in further derived reports.

The application runs in three distinct steps:

1. Retreive desired Canvas table data from DAP in JSONL format
2. Cleanup data from JSONL data files using pandas and export to final CSV files
3. Insert data from final CSV data files into Oracle tables

For our example setup below, we are looking to implement an early-alert system for students struggling in Canvas courses, so that we can forward them to Advising or other resources for assistance. We have retreived data from Canvas that is used to create tables and a view that we can use to gauge student's performance in their current Canvas course enrollments through their overall course score. Supporting information like their total time spent in the enrollment and their last activity date in the enrollment can help identify struggling students that may require assistance from Advising, etc.

## Requirements

This application assumes you already have an Oracle Database and Canvas LMS in place.

- [Oracle Database](https://www.oracle.com/database/)
- [Python](https://www.python.org/)
- [Canvas LMS](https://www.instructure.com/canvas)

## Setup

### Application setup

To begin, clone or download the canvas-data-integration application. For the application setup, proceed as follows:

1. (Optional) Set up a Python virtual environment to not pollute the base environment on the machine hosting the application.
    1. Navigate to the application directory
    2. Setup a virtual environment: `python -m venv .venv`
    3. Activate the virtual environment (to install required packages in it): `.\.venv\Scripts\activate`
2. Next, navigate to the application directory (if not already there) and install the required Python packages for the application: `python -m pip install -r requirements.txt`
3. Next, either create system environment variables or create an environment variable file called `.env` alongside the `config.yml` file in the application's base directory, named like so: `canva-data-integration/.env`. We use environment variables for sensitive but necessary DAP and database authentication fields, those being:

    ```properties
    DAP_API_URL=my-api-gateway-url
    DAP_CLIENT_ID='my-client-id' # in single quotes to escape hashtags
    DAP_CLIENT_SECRET=my-client-secret
    DB_HOST=my-host
    DB_PORT=my-port-number
    DB_SERVICE=my-service-name
    DB_USERNAME=my-oracle-username
    DB_PASSWORD=my-oracle-password
    ```

    Replace the values with your own connection and authentiation information for DAP and Oracle. DAP API tokens can be obtained at [identity.instructure.com](https://identity.instructure.com/login), but are temporary and will need to be refreshed occasionally.
4. Change the directory in `run.ps1` to your project directory

---

### Oracle setup

It is assumed that you are already aware of the Canvas data structure you are looking to retrieve. Use [DAP Datasets](https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#datasets) to identify Canvas tables and fields you wish to retrieve. For the database table, view, and merge query setup, proceed as follows:

1. Create your Canvas table equivalents in Oracle with your desired columns. Oracle scripts can be derived and condensed from DAP API scripts for [PostgreSQL](https://data-access-platform-api.s3.eu-central-1.amazonaws.com/sql/postgresql.sql), [MySQL](https://data-access-platform-api.s3.eu-central-1.amazonaws.com/sql/mysql.sql), [Microsoft SQL Server](https://data-access-platform-api.s3.amazonaws.com/sql/mssql.sql). The sample statements below are the tables we need for our early-alert system:

    **canvas_course_sections** - [DAP dataset course_sections](https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#schemas.canvas.course_sections)

    ```sql
    CREATE TABLE canvas_course_sections (
        course_sections_id NUMBER(19) NOT NULL,
        course_sections_name VARCHAR2(255) NOT NULL,
        course_sections_course_id NUMBER(19) NOT NULL,
        course_sections_workflow_state VARCHAR2(255) NOT NULL,
        course_sections_ts TIMESTAMP ZONE NOT NULL,
        CONSTRAINT pk_course_sections PRIMARY KEY (course_sections_id)
        );
    ```

    **canvas_courses** - [DAP dataset courses](https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#schemas.canvas.courses)

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

    **canvas_enrollment_terms** - [DAP dataset enrollment_terms](https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#schemas.canvas.enrollment_terms)

    ```sql
    CREATE TABLE canvas_enrollment_terms (
        enrollment_terms_id NUMBER(19) NOT NULL,
        enrollment_terms_sis_source_id VARCHAR2(255),
        enrollment_terms_workflow_state VARCHAR2(255) NOT NULL,
        enrollment_terms_ts TIMESTAMP NOT NULL,
        CONSTRAINT pk_enrollment_terms PRIMARY KEY (enrollment_terms_id)
        );
    ```  

    **canvas_enrollments** - [DAP dataset enrollments](https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#schemas.canvas.enrollments)

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

    **canvas_pseudonyms** - [DAP dataset pseudonyms](https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#schemas.canvas.pseudonyms)

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

    **canvas_scores** - [DAP dataset scores](https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#schemas.canvas.scores)

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

    **canvas_users** - [DAP dataset users](https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#schemas.canvas.users)

    ```sql
    CREATE TABLE canvas_users (
        users_id NUMBER(19) NOT NULL,
        users_workflow_state VARCHAR2(255) NOT NULL,
        users_name VARCHAR2(255),
        users_ts TIMESTAMP NOT NULL,
        CONSTRAINT pk_users PRIMARY KEY (users_id)
        );
    ```

2. (Optional) In our case, we also want a final view for our joined Canvas table information to list out distinct Canvas student enrollments with all our desired data points:

    **canvas_data**

    ```sql
    create or replace view canvas_data as
    select distinct
            substr(et.enrollment_terms_sis_source_id, 0, 6) TermCode,
            substr(et.enrollment_terms_sis_source_id, 8) PartOfTerm, 
            substr(c.courses_sis_source_id, 8) CRN,
            substr(substr(cs.course_sections_name, 1, instr(cs.course_sections_name, '_', -1, 1) -1), instr(substr(cs.course_sections_name, 1, instr(cs.course_sections_name, '_', -1, 1) -1), '_', -1, 1) +1) SubjectCodeCourseNumber,
            substr(cs.course_sections_name, instr(cs.course_sections_name, '_', -1, 1) +1) CourseName,
            to_char(e.enrollments_last_activity_at,'MM/DD/YYYY') LastActivityDate,
            round(e.enrollments_total_activity_time/60/60, 2) TotalActivityTimeHrs,
            u.users_name FullName,
            p.pseudonyms_sis_user_id XID,
            p.pseudonyms_unique_id Email,
            to_char(s.scores_current_score, 'FM99999990.00') CurrentScore,
            e.enrollments_workflow_state EnrollmentState,
            LISTAGG(tp.pseudonyms_sis_user_id, ', ') WITHIN GROUP (ORDER BY tp.pseudonyms_sis_user_id) AS TeacherXID,
            LISTAGG(tu.users_name, ', ') WITHIN GROUP (ORDER BY tu.users_name) AS TeacherFullName 
    from    canvas_enrollment_terms et
    join    canvas_courses c on c.courses_enrollment_term_id = et.enrollment_terms_id
    join    canvas_course_sections cs on cs.course_sections_course_id = c.courses_id
    join    canvas_enrollments e on e.enrollments_course_section_id = cs.course_sections_id
        and e.enrollments_course_id = c.courses_id
        and e.enrollments_workflow_state = 'active' -- active courses only
    join    canvas_users u on u.users_id = e.enrollments_user_id
    join    canvas_pseudonyms p on p.pseudonyms_user_id = u.users_id
        and p.pseudonyms_id = e.enrollments_sis_pseudonym_id
    join    canvas_scores s on s.scores_enrollment_id = e.enrollments_id
        and s.scores_course_score = 'True'
    left join canvas_enrollments te on te.enrollments_course_section_id = cs.course_sections_id
        and te.enrollments_course_id = c.courses_id
        and te.enrollments_type = 'TeacherEnrollment'
    left join canvas_users tu on tu.users_id = te.enrollments_user_id
    left join canvas_pseudonyms tp on tp.pseudonyms_user_id = tu.users_id
        and tp.pseudonyms_id = te.enrollments_sis_pseudonym_id
    where   et.enrollment_terms_sis_source_id like '20%'
    group by substr(et.enrollment_terms_sis_source_id, 0, 6),
            substr(et.enrollment_terms_sis_source_id, 8),
            substr(c.courses_sis_source_id, 8),
            substr(substr(cs.course_sections_name, 1, instr(cs.course_sections_name, '_', -1, 1) -1), instr(substr(cs.course_sections_name, 1, instr(cs.course_sections_name, '_', -1, 1) -1), '_', -1, 1) +1),
            substr(cs.course_sections_name, instr(cs.course_sections_name, '_', -1, 1) +1),
            to_char(e.enrollments_last_activity_at,'MM/DD/YYYY'),
            round(e.enrollments_total_activity_time/60/60, 2),
            u.users_name,
            p.pseudonyms_sis_user_id,
            p.pseudonyms_unique_id,
            to_char(s.scores_current_score, 'FM99999990.00'),
            e.enrollments_workflow_state;
    ```

3. In the application directory, modify `config.yaml` to contain a `canvas_table` configuration entry with the details of each table as demonstrated in the sample [`config.yml`](config.yml).
    - For each table, `fields` accepts a list of desired columns from the Canvas table as defined in DAP datasets. See `config.yml` for examples.
    - The `db_query` field should define your merge query that will update your Oracle table with the newest Canvas table information from each application run. See `config.yml` for examples.
    - The `query_type` field ('incremental' or 'snapshot') defines which time-period DAP should retreive data for, for the specified Canvas table, as defined [here](https://data-access-platform-api.s3.amazonaws.com/client/README.html#getting-latest-changes-with-an-incremental-query). When intializing your Oracle database tables, it is recommended to first run each table in 'snapshot' mode to get the totality of records from the Canvas table from DAP. ***Warning**: Certain Canvas tables can return large numbers of records when using 'snapshot' mode. You can test with 'incremental' mode first to see how many records are returned for a more specific period of time.*
        - Afterwards, you can retreive the records changed in the past X days with the 'incremental' mode in combination with the `past_days` configuration entry.

4. (Optional) Timestamps retrieved from Canvas are formatted according to [ISO-8601 standards and are in UTC time zone](https://data-access-platform-api.s3.amazonaws.com/index.html#section/Data-representation/Metadata). These timestamps are used solely for comparison purposes in Oracle `MERGE` queries that insert or update data in our Oracle tables. Therefore, you can safely insert them directly into the corresponding `TIMESTAMP` fields in the tables. Should you wish to convert to your local time zone for further operations,  you can adjust the setup as follows:
    1. Modify each table's timestamp field to use the `TIMESTAMP WITH TIME ZONE` data type instead of the `TIMESTAMP` data type.
    2. Adjust your `MERGE` queries to convert your timestamp fields to a different time zone. For example:

        ```sql
        TO_TIMESTAMP(:4, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
        
        to

        FROM_TZ(TO_TIMESTAMP(:4, 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'),'UTC') AT TIME ZONE 'America/New_York'
        ```

## Resources

- [Instructure API Gateway (0.7.3) - Docs](https://api-gateway.instructure.com/doc/)
- [Instructure Identity Services - Get DAP API tokens](https://identity.instructure.com/login)
- [Data Access Platform Query API (1.0.0)](https://data-access-platform-api.s3.amazonaws.com/index.html)
- [Data Access Platform Client Library](https://data-access-platform-api.s3.amazonaws.com/client/index.html)
- [Canvas LMS Community - Generating SQL Create Table Scripts from JSON Schemas for Canvas Data 2](https://community.canvaslms.com/t5/Data-and-Analytics-GroupGenerating-SQL-Create-Table-Scripts-from-JSON-Schemas-for-Canvas/m-p/588305)
- [Canvas LMS Community - DAP API and API key vs. client ID + secret](https://community.canvaslms.com/t5/Data-and-Analytics-Group/DAP-API-and-API-key-vs-client-ID-secret-please-clarify-if-any/m-p/568180)
