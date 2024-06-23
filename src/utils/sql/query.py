class QueryStorage:
        
    check_for_schedule_existance = """
                                    SELECT s."SubredditId", ss."SubredditScheduleId", ss."LastParsedDate" 
                                    FROM "Reddit"."Subreddit" s
                                    LEFT JOIN "Reddit"."SubredditSchedule" ss ON s."SubredditId" = ss."SubredditId"
                                    WHERE s."SubredditName" =  '{subreddit_path}' and ss."IsActive" = true
                                    ORDER BY s."SubredditId" desc
                                    """


    insert_subreddit = """ INSERT INTO "Reddit"."Subreddit"("SubredditName") VALUES ('{subreddit_path}') returning "SubredditId" """

    insert_subreddit_schedule = """ INSERT INTO "Reddit"."SubredditSchedule"("SubredditId", "LastParsedDate") VALUES({subreddit_id}, Null )  returning "SubredditScheduleId" """

    update_schedule_status_running = """ UPDATE "Reddit"."SubredditSchedule" SET  "LastCheckStatus" = 'running' WHERE "SubredditId" = {subreddit_id} """

    update_end_subreddit_schedule_date_query = """ UPDATE "Reddit"."SubredditSchedule" SET "LastParsedDate" = NOW() , "LastCheckStatus" = '{status}' WHERE "SubredditScheduleId" = {subreddit_schedule_id} """

    update_end_subreddit_schedule_status = """ UPDATE "Reddit"."SubredditSchedule" SET  "LastCheckStatus" = '{status}' WHERE "SubredditScheduleId" = {subreddit_schedule_id} """

    mid_update_subreddit_job_status = """update "Reddit"."SubredditJob" set 
                        "SubredditId" =  {subreddit_id} ,
                        "JobTypeId" = (select "JobTypeId"  from "Reddit"."JobType" where LOWER("JobTypeName") = LOWER( '{execution_type}' ) ),
                        "TopicId" = {topic_id} 
                        where "SubredditJobId" = {subreddit_job_id}
                        """
    kafka_topic_select_query = """
                select kt."TopicId" from "Reddit"."KafkaTopic" kt 
                where kt."TopicName" =  '{topic_name}'
            """

    kafka_insert_query = """
                insert into "Reddit"."KafkaTopic"("TopicName") VALUES('{topic_name}') returning "TopicId"
            """