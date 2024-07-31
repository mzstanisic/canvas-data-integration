# file needs to start at current term
import os
import asyncio
import gzip
import shutil
import glob
from pathlib import Path
from datetime import datetime, timedelta, timezone
from urllib.parse import ParseResult, urlparse
import pandas as pd

import helper
import aiofiles

import yaml
from dotenv import load_dotenv, find_dotenv

from dap.api import DAPClient
from dap.dap_types import Credentials
from dap.dap_types import Format, SnapshotQuery, IncrementalQuery


CONFIG_DIR = os.path.dirname(__file__) + '/../config.yml'
config = yaml.safe_load(open(CONFIG_DIR))
# TODO: local environment variables, remove when deployed
load_dotenv(os.path.dirname(__file__) + '/../resources/.env')
base_url: str = os.environ.get("DAP_API_URL")
client_id: str = os.environ.get("DAP_CLIENT_ID")
client_secret: str = os.environ.get("DAP_CLIENT_SECRET")

# TODO: on-machine environment variables, uncomment when deployed
# base_url: str = os.environ["DAP_API_URL"]
# client_id: str = os.environ["DAP_CLIENT_ID"]
# client_secret: str = os.environ["DAP_CLIENT_SECRET"]
credentials = Credentials.create(client_id=client_id, client_secret=client_secret)

prefix_path: str = os.path.dirname(__file__) + config.get('prefix_path') # TODO: rename all config variables
final_path: str = os.path.dirname(__file__) + config.get('final_path') # TODO: rename all config variables
output_format = helper.get_format(config) # TODO: rename all config variables
cfg_query_type: str = "incremental" # TODO: establish this in the config
# prefix_path = "V:/BI Project/Temp Files"
# final_path = "V:/BI Project/New Files for Devin"


# timestamp returned by last snapshot or incremental query
last_seen = datetime.now(timezone.utc) - timedelta(days=1)
# last_seen = datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc)

# clear temp files from subfolders
for temps in glob.glob(os.path.join(prefix_path, "**"), recursive=True):
    if temps.endswith((".csv", ".gz")): #TODO: refactor into helper function, add tsv and parquet
        os.remove(temps)



async def get_canvas_data(table: str, output_directory: str, format=Format.CSV, query_type="incremental") -> None:
    """
    Retrieves data files from Canvas for the specified Canvas table.

    :param table: A Canvas table: https://data-access-platform-api.s3.amazonaws.com/tables/catalog.html#datasets
    :param output_directory: The output directory for the generated data files.
    :param format: The desired format for the data files: `CSV`, `JSONL`, `TSV`, or `Parquet`
    """

    if format == Format.CSV:
        output_directory = output_directory + "/csv"
    elif format == Format.JSONL:
        output_directory = output_directory + "/json"
    elif format == Format.TSV:
        output_directory = output_directory + "/tsv"
    elif format == Format.Parquet:
        output_directory = output_directory + "/parquet"

    async with DAPClient() as session:
        if query_type == "snapshot":
            query = SnapshotQuery(format=format, mode=None)
        elif query_type == "incremental":
            query = IncrementalQuery(format=format, mode=None, since=last_seen, until=None)

        query_object = await session.get_table_data("canvas", table, query)
        filename = await session.download_object(query_object.objects[0], output_directory, decompress=True)

        p = Path(filename)
        p.rename(p.with_stem(table))
        
    


async def update_courses():
    async with DAPClient() as session:
        query = IncrementalQuery(
            format=Format.CSV,
            filter=None,
            since=last_seen,
            until=None,
        )
        result = await session.get_table_data("canvas", "courses", query)
        resources = await session.get_resources(result.objects)
        for resource in resources:
            components: ParseResult = urlparse(str(resource.url))
            file_path = os.path.join(
                prefix_path, "Courses", os.path.basename(components.path)
            )
            async with session.stream_resource(resource) as stream:
                async with aiofiles.open(file_path, "wb") as file:
                    # save gzip data to file without decompressing
                    async for chunk in stream.iter_chunked(64 * 1024):
                        await file.write(chunk)
        # Remove empty gzip
        fileList = glob.glob(os.path.join(prefix_path, "Courses", "*.gz"))
        for filename in fileList:
            if os.stat(filename).st_size < 1 * 1024:
                os.remove(filename)
        # Refresh file list, then extract from gzip and rename csv
        new_file_list = glob.glob(os.path.join(prefix_path, "Courses", "*.gz"))
        for new_file_name in new_file_list:
            formatted_file_name = new_file_name.replace("\\", "/")
        with gzip.open(formatted_file_name, "rb") as f_in:
            with open(os.path.join(prefix_path, "temp-courses.csv"), "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        # Merge existing file with new extract and remove duplicates
        df1 = pd.read_csv(os.path.join(final_path, "canvas-courses.csv"))
        df2 = pd.read_csv(
            os.path.join(prefix_path, "temp-courses.csv"),
            dtype={
                "value.tab_configuration": object,
                "value.turnitin_comments": object,
            },
        )
        df2.drop(
            [
                "value.syllabus_body",
                "value.storage_quota",
                "value.integration_id",
                "value.lti_context_id",
                "value.sis_batch_id",
                "value.created_at",
                "value.updated_at",
                "value.workflow_state",
                "value.account_id",
                "value.grading_standard_id",
                "value.start_at",
                "value.sis_source_id",
                "value.group_weighting_scheme",
                "value.conclude_at",
                "value.is_public",
                "value.allow_student_wiki_edits",
                "value.default_wiki_editing_roles",
                "value.wiki_id",
                "value.allow_student_organized_groups",
                "value.course_code",
                "value.default_view",
                "value.abstract_course_id",
                "value.open_enrollment",
                "value.tab_configuration",
                "value.turnitin_comments",
                "value.self_enrollment",
                "value.license",
                "value.indexed",
                "value.restrict_enrollments_to_course_dates",
                "value.template_course_id",
                "value.replacement_course_id",
                "value.public_description",
                "value.self_enrollment_code",
                "value.self_enrollment_limit",
                "value.turnitin_id",
                "value.show_announcements_on_home_page",
                "value.home_page_announcement_limit",
                "value.latest_outcome_import_id",
                "value.grade_passback_setting",
                "value.template",
                "value.homeroom_course",
                "value.sync_enrollments_from_homeroom",
                "value.homeroom_course_id",
                "value.locale",
                "value.time_zone",
                "value.uuid",
                "value.settings.allow_student_discussion_editing",
                "value.settings.allow_student_discussion_topics",
                "value.settings.course_format",
                "value.settings.filter_speed_grader_by_student_group",
                "value.settings.hide_distribution_graphs",
                "value.settings.hide_final_grade",
                "value.settings.is_public_to_auth_users",
                "value.settings.lock_all_announcements",
                "value.settings.public_syllabus",
                "value.settings.public_syllabus_to_auth",
                "value.settings.restrict_student_future_view",
                "value.settings.restrict_student_past_view",
                "value.settings.syllabus_updated_at",
                "value.settings.usage_rights_required",
                "value.settings.allow_student_forum_attachments",
            ],
            axis=1,
            inplace=True,
        )
        df_merged = pd.concat([df1, df2])
        dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
        dupes_removed.to_csv(
            os.path.join(final_path, "canvas-courses.csv"), index=False
        )


async def update_course_sections():
    async with DAPClient() as session:
        query = IncrementalQuery(
            format=Format.CSV,
            filter=None,
            since=last_seen,
            until=None,
        )
        result = await session.get_table_data("canvas", "course_sections", query)
        resources = await session.get_resources(result.objects)
        for resource in resources:
            components: ParseResult = urlparse(str(resource.url))
            file_path = os.path.join(
                prefix_path, "Course Sections", os.path.basename(components.path)
            )
            async with session.stream_resource(resource) as stream:
                async with aiofiles.open(file_path, "wb") as file:
                    # save gzip data to file without decompressing
                    async for chunk in stream.iter_chunked(64 * 1024):
                        await file.write(chunk)
        with gzip.open(file_path, "rb") as f_in:
            with open(
                os.path.join(prefix_path, "temp-course_sections.csv"), "wb"
            ) as f_out:
                shutil.copyfileobj(f_in, f_out)
        # merge existing file with new extract
        df1 = pd.read_csv(os.path.join(final_path, "canvas-course_sections.csv"))
        df2 = pd.read_csv(os.path.join(prefix_path, "temp-course_sections.csv"))
        df2.drop(
            [
                "value.integration_id",
                "value.created_at",
                "value.updated_at",
                "value.workflow_state",
                "value.sis_batch_id",
                "value.start_at",
                "value.end_at",
                "value.sis_source_id",
                "value.default_section",
                "value.accepting_enrollments",
                "value.restrict_enrollments_to_section_dates",
                "value.nonxlist_course_id",
            ],
            axis=1,
            inplace=True,
        )
        df_merged = pd.concat([df1, df2])
        dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
        dupes_removed.to_csv(
            os.path.join(final_path, "canvas-course_sections.csv"), index=False
        )


async def update_enrollment_terms(output_format=Format.CSV):
    async with DAPClient() as session:
        query = IncrementalQuery(
            # format=Format.CSV,
            format=output_format,
            # filter=None,
            mode=None,
            since=last_seen,
            until=None,
        )
        result = await session.get_table_data("canvas", "enrollment_terms", query)
        resources = await session.get_resources(result.objects)
        for resource in resources.values():
            components: ParseResult = urlparse(str(resource.url))
            file_path = os.path.join(
                prefix_path, "Enrollment Terms", os.path.basename(components.path)
            )
            # async with session.stream_resource(resource) as stream:
            async for stream in session.stream_resource(resource):
                async with aiofiles.open(file_path, "wb") as file:
                    # save gzip data to file without decompressing
                    async for chunk in stream.iter_chunked(64 * 1024):
                        await file.write(chunk)
        with gzip.open(file_path, "rb") as f_in:
            with open(
                os.path.join(prefix_path, "temp-enrollment_terms.csv"), "wb"
            ) as f_out:
                shutil.copyfileobj(f_in, f_out)
        # merge existing file with new extract
        df1 = pd.read_csv(os.path.join(final_path, "canvas-enrollment_terms.csv"))
        df2 = pd.read_csv(os.path.join(prefix_path, "temp-enrollment_terms.csv"))
        df2.drop(
            [
                # "value.integration_id",
                "value.created_at",
                "value.updated_at",
                "value.workflow_state",
                "value.sis_batch_id",
                "value.start_at",
                "value.end_at",
                # "value.sis_source_id",
                "value.grading_period_group_id",
            ],
            axis=1,
            inplace=True,
        )
        df_merged = pd.concat([df1, df2])
        dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
        dupes_removed.to_csv(
            os.path.join(final_path, "canvas-enrollment_terms.csv"), index=False
        )


async def update_enrollments():
    async with DAPClient() as session:
        query = IncrementalQuery(
            format=Format.CSV,
            filter=None,
            since=last_seen,
            until=None,
        )
        result = await session.get_table_data("canvas", "enrollments", query)
        resources = await session.get_resources(result.objects)
        for resource in resources:
            components: ParseResult = urlparse(str(resource.url))
            file_path = os.path.join(
                prefix_path, "enrollments", os.path.basename(components.path)
            )
            async with session.stream_resource(resource) as stream:
                async with aiofiles.open(file_path, "wb") as file:
                    # save gzip data to file without decompressing
                    async for chunk in stream.iter_chunked(64 * 1024):
                        await file.write(chunk)
        # Remove empty gzip
        fileList = glob.glob(os.path.join(prefix_path, "Enrollments", "*.gz"))
        for filename in fileList:
            if os.stat(filename).st_size < 1 * 1024:
                os.remove(filename)
        # Refresh file list, then extract from gzip and rename csv
        new_file_list = glob.glob(os.path.join(prefix_path, "Enrollments", "*.gz"))
        for new_file_name in new_file_list:
            formatted_file_name = new_file_name.replace("\\", "/")
        with gzip.open(formatted_file_name, "rb") as f_in:
            with open(os.path.join(prefix_path, "temp-enrollments.csv"), "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        # merge existing file with new extract
        df1 = pd.read_csv(os.path.join(final_path, "canvas-enrollments.csv"))
        df2 = pd.read_csv(os.path.join(prefix_path, "temp-enrollments.csv"))
        # df1.drop(['value.start_at','value.end_at','value.completed_at','value.grade_publishing_status','value.associated_user_id','value.self_enrolled','value.limit_privileges_to_course_section','value.total_activity_time','value.last_attended_at'],axis=1,inplace=True)
        df2.drop(
            [
                "value.sis_batch_id",
                "value.user_id",
                "value.created_at",
                "value.updated_at",
                "value.workflow_state",
                "value.role_id",
                "value.start_at",
                "value.end_at",
                "value.completed_at",
                "value.grade_publishing_status",
                "value.associated_user_id",
                "value.self_enrolled",
                "value.limit_privileges_to_course_section",
                "value.total_activity_time",
                "value.last_attended_at",
                "value.type",
            ],
            axis=1,
            inplace=True,
        )
        df_merged = pd.concat([df1, df2])
        dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
        dupes_removed.to_csv(
            os.path.join(final_path, "canvas-enrollments.csv"), index=False
        )


async def update_pseudonyms():
    async with DAPClient() as session:
        query = IncrementalQuery(
            format=Format.CSV,
            filter=None,
            since=last_seen,
            until=None,
        )
        result = await session.get_table_data("canvas", "pseudonyms", query)
        resources = await session.get_resources(result.objects)
        for resource in resources:
            components: ParseResult = urlparse(str(resource.url))
            file_path = os.path.join(
                prefix_path, "Pseudonyms", os.path.basename(components.path)
            )
            async with session.stream_resource(resource) as stream:
                async with aiofiles.open(file_path, "wb") as file:
                    # save gzip data to file without decompressing
                    async for chunk in stream.iter_chunked(64 * 1024):
                        await file.write(chunk)
        with gzip.open(file_path, "rb") as f_in:
            with open(os.path.join(prefix_path, "temp-pseudonyms.csv"), "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        # merge existing file with new extract
        df1 = pd.read_csv(os.path.join(final_path, "canvas-pseudonyms.csv"))
        df2 = pd.read_csv(os.path.join(prefix_path, "temp-pseudonyms.csv"))
        df2.drop(
            [
                "value.deleted_at",
                "value.integration_id",
                "value.created_at",
                "value.updated_at",
                "value.workflow_state",
                "value.account_id",
                "value.sis_batch_id",
                "value.unique_id",
                "value.login_count",
                "value.failed_login_count",
                "value.last_request_at",
                "value.last_login_at",
                "value.current_login_at",
                "value.last_login_ip",
                "value.current_login_ip",
                "value.authentication_provider_id",
                "value.position",
            ],
            axis=1,
            inplace=True,
        )
        df_merged = pd.concat([df1, df2])
        dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
        dupes_removed.to_csv(
            os.path.join(final_path, "canvas-pseudonyms.csv"), index=False
        )


async def update_scores():
    async with DAPClient() as session:
        query = IncrementalQuery(
            format=Format.CSV,
            filter=None,
            since=last_seen,
            until=None,
        )
        result = await session.get_table_data("canvas", "scores", query)
        resources = await session.get_resources(result.objects)
        for resource in resources:
            components: ParseResult = urlparse(str(resource.url))
            file_path = os.path.join(
                prefix_path, "scores", os.path.basename(components.path)
            )
            async with session.stream_resource(resource) as stream:
                async with aiofiles.open(file_path, "wb") as file:
                    # save gzip data to file without decompressing
                    async for chunk in stream.iter_chunked(64 * 1024):
                        await file.write(chunk)
        # Remove empty gzip
        fileList = glob.glob(os.path.join(prefix_path, "Scores", "*.gz"))
        for filename in fileList:
            if os.stat(filename).st_size < 1 * 1024:
                os.remove(filename)
        # Refresh file list, then extract from gzip and rename csv
        new_file_list = glob.glob(os.path.join(prefix_path, "Scores", "*.gz"))
        for new_file_name in new_file_list:
            formatted_file_name = new_file_name.replace("\\", "/")
        with gzip.open(formatted_file_name, "rb") as f_in:
            with open(os.path.join(prefix_path, "temp-scores.csv"), "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        # merge existing file with new extract
        df1 = pd.read_csv(os.path.join(final_path, "canvas-scores2.csv"))
        df2 = pd.read_csv(
            os.path.join(prefix_path, "temp-scores.csv"), dtype={"meta.ts": str}
        )
        df2.drop(
            [
                "value.created_at",
                "value.updated_at",
                "value.workflow_state",
                "value.assignment_group_id",
                "value.grading_period_id",
                "value.final_score",
                "value.unposted_current_score",
                "value.unposted_final_score",
                "value.current_points",
                "value.unposted_current_points",
                "value.final_points",
                "value.unposted_final_points",
                "value.override_score",
            ],
            axis=1,
            inplace=True,
        )
        df_merged = pd.concat([df1, df2])
        dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
        dupes_removed.to_csv(
            os.path.join(final_path, "canvas-scores2.csv"), index=False
        )


async def update_users():
    async with DAPClient() as session:
        query = IncrementalQuery(
            format=Format.CSV,
            filter=None,
            since=last_seen,
            until=None,
        )
        result = await session.get_table_data("canvas", "users", query)
        resources = await session.get_resources(result.objects)
        for resource in resources:
            components: ParseResult = urlparse(str(resource.url))
            file_path = os.path.join(
                prefix_path, "Users", os.path.basename(components.path)
            )
            async with session.stream_resource(resource) as stream:
                async with aiofiles.open(file_path, "wb") as file:
                    # save gzip data to file without decompressing
                    async for chunk in stream.iter_chunked(64 * 1024):
                        await file.write(chunk)
        with gzip.open(file_path, "rb") as f_in:
            with open(os.path.join(prefix_path, "temp-users.csv"), "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        # merge existing file with new extract
        df1 = pd.read_csv(os.path.join(final_path, "canvas-users.csv"))
        df2 = pd.read_csv(os.path.join(prefix_path, "temp-users.csv"))
        df2.drop(
            [
                "value.deleted_at",
                "value.storage_quota",
                "value.lti_context_id",
                "value.created_at",
                "value.updated_at",
                "value.workflow_state",
                "value.sortable_name",
                "value.avatar_image_url",
                "value.avatar_image_source",
                "value.avatar_image_updated_at",
                "value.short_name",
                "value.last_logged_out",
                "value.pronouns",
                "value.merged_into_user_id",
                "value.locale",
                "value.time_zone",
                "value.uuid",
                "value.school_name",
                "value.school_position",
                "value.public",
            ],
            axis=1,
            inplace=True,
        )
        df_merged = pd.concat([df1, df2])
        dupes_removed = df_merged.drop_duplicates("key.id", keep="last", inplace=False)
        dupes_removed.to_csv(os.path.join(final_path, "canvas-users.csv"), index=False)


async def update_all():
    await get_canvas_data("enrollment_terms", prefix_path, output_format, "snapshot") #TODO: enable after testing #cfg_query_type)

    # await update_courses()
    # await update_course_sections()
    # await update_enrollment_terms(output_format)
    # await update_enrollments()
    # await update_pseudonyms()
    # await update_scores()
    # await update_users()


async def main():
    task = asyncio.create_task(update_all())
    await task


asyncio.run(main())
