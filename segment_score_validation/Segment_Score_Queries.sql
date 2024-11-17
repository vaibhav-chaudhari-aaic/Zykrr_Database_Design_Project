WITH campaignIds AS (SELECT child AS id
                            FROM "GroupedCampaignChildren"
                            WHERE parent = 'c8afefb2-ed74-43be-a722-f0b0b78dd939'
                            UNION
                            SELECT 'c8afefb2-ed74-43be-a722-f0b0b78dd939' as id),
            questionConfigs AS (SELECT jsonb_array_elements(questionnaire) as questionConfig, id AS campaignId
                                FROM "Campaigns"
                                WHERE id IN (SELECT id FROM campaignIds)),
            npsQuestion AS (SELECT questionConfig ->> 'id' AS qId, campaignId
                            FROM questionConfigs
                            WHERE (questionConfig -> 'config' -> 'isNPSQuestion')::boolean =
                                  true),
            npsMetricConfigs AS ( SELECT * from "MetricConfigs" where "MetricConfigs"."campaignId" IN (SELECT id FROM campaignIds) AND type = 'NPS' ),
            pivotQuestions AS (SELECT questionConfig ->> 'id' AS "id", campaignId
                               FROM questionConfigs
                               WHERE questionConfig -> 'config' ->> 'attributeName' = 'Speciality'
                               AND (questionConfig -> 'config' -> 'isDimension')::boolean = true),
            trend AS (
                      SELECT answers -> pivotQuestions.id ->> 0 AS value, -- ASSUMING pivotQuestionValue has only one element in the json array
                      SUM(CASE WHEN (answers ->> npsQuestion.qId)::int > npsMetricConfigs."promoterLowerRange" THEN 1 ELSE 0 END)::int as "promoters",
                      SUM(CASE WHEN (answers ->> npsQuestion.qId)::int > npsMetricConfigs."detractorUpperRange" AND (answers ->> npsQuestion.qId)::int <= npsMetricConfigs."promoterLowerRange"  THEN 1 ELSE 0 END)::int as "passives",
                      SUM(CASE WHEN (answers ->> npsQuestion.qId)::int <= npsMetricConfigs."detractorUpperRange" THEN 1 ELSE 0 END)::int as "detractors"

                        ,date_part('week', "Responses"."createdAt" at time zone '-5:30') AS week


                        ,date_part('isoyear', "Responses"."createdAt" at time zone '-5:30') AS year

                      FROM "Responses"
                      INNER JOIN npsQuestion ON "Responses"."campaignId" = npsQuestion.campaignId
                      INNER JOIN pivotQuestions ON "Responses"."campaignId" = pivotQuestions.campaignId
                      INNER JOIN npsMetricConfigs ON "Responses"."campaignId" = npsMetricConfigs."campaignId"
                      WHERE "Responses"."campaignId" IN (SELECT id FROM campaignIds) AND "Responses"."createdAt" BETWEEN '2023-04-01 00:00:00.000 +05:30' AND '2023-11-06 23:59:59.999 +05:30'
                      AND answers -> pivotQuestions.id ->> 0 IS NOT NULL
                      AND answers ->> npsQuestion.qId IS NOT NULL

            GROUP BY value, year, week
            ORDER BY year, week )
      SELECT *,(promoters + passives + detractors) AS all,
      round( CAST( (promoters - detractors)::float / (promoters + passives + detractors) * 100 AS numeric),1) AS "netPromoterScore"
      FROM trend
      WHERE  (promoters + passives + detractors) <> 0

------------------------------------------------------------------------------





-------------------------------------------------------------------------
-------------------------------------------------------------------------





WITH campaignIds AS (SELECT child AS id
                FROM "GroupedCampaignChildren"
                WHERE parent = 'c8afefb2-ed74-43be-a722-f0b0b78dd939'
                UNION
                SELECT 'c8afefb2-ed74-43be-a722-f0b0b78dd939' as id),
          npsMetricConfigs AS ( SELECT * from "MetricConfigs" where "MetricConfigs"."campaignId" IN (SELECT id FROM campaignIds) AND type = 'NPS' ),
          questionConfigs AS (SELECT jsonb_array_elements(questionnaire) as questionConfig, id AS "campaignId"
                    FROM "Campaigns"
                    WHERE id IN (SELECT id FROM campaignIds)),
          questionsId AS (SELECT questionConfig ->> 'id' AS id, "campaignId"
                    FROM questionConfigs
                    WHERE (questionConfig -> 'config' -> 'isNPSQuestion')::boolean = true),
          pivotQuestions AS (SELECT questionConfig ->> 'id' AS "id", "campaignId"
                    FROM questionConfigs
                    WHERE questionConfig -> 'config' ->> 'attributeName' = 'Speciality'
                    AND (questionConfig -> 'config' -> 'isDimension')::boolean = true),
          segmentCountPerKeyword AS (
            SELECT "Responses".id, answers -> pivotQuestions.id ->> 0 AS value, "Responses"."campaignId",
            round(avg(NULLIF(answers ->> questionsId.id,'NA')::int), 1)::float AS "score"
            FROM "Responses"
            INNER JOIN questionsId ON "Responses"."campaignId" = questionsId."campaignId"
            INNER JOIN pivotQuestions ON "Responses"."campaignId" = pivotQuestions."campaignId"
            WHERE "Responses"."campaignId" IN (SELECT id FROM campaignIds) AND "Responses"."createdAt" BETWEEN '2023-04-01 00:00:00.000 +05:30' AND '2023-11-06 23:59:59.999 +05:30'
            AND answers -> pivotQuestions.id ->> 0 IS NOT NULL
            AND answers ->> questionsId.id IS NOT NULL
            GROUP BY value,"Responses".id
          ),
          countPerPivot AS (
            SELECT value,segmentCountPerKeyword.id,
            CASE
              WHEN segmentCountPerKeyword.score > npsMetricConfigs."promoterLowerRange" THEN 'HIGHLY_SATISFIED'
              WHEN segmentCountPerKeyword.score > npsMetricConfigs."detractorUpperRange" THEN 'SATISFIED'
              WHEN segmentCountPerKeyword.score <= npsMetricConfigs."detractorUpperRange" THEN 'UNSATISFIED'
            END  AS segment
            FROM segmentCountPerKeyword
            INNER JOIN npsMetricConfigs on segmentCountPerKeyword."campaignId" = npsMetricConfigs."campaignId"
            GROUP BY value,score, segmentCountPerKeyword."id",score,npsMetricConfigs."promoterLowerRange",npsMetricConfigs."detractorUpperRange"
            ORDER BY score
          )
          SELECT value, segment ,count(*) FROM countPerPivot
          GROUP BY value,segment





-------------------------------------------------------------------------
-------------------------------------------------------------------------
f9c41cdc-4352-4769-8125-f5ab59cf60bc
a827673c-12d4-473d-9d07-aea8d4e3e0dd


  


WITH campaignIds AS (SELECT child AS id
                     FROM "GroupedCampaignChildren"
                     WHERE parent = 'c8afefb2-ed74-43be-a722-f0b0b78dd939'
                     UNION
                     SELECT 'c8afefb2-ed74-43be-a722-f0b0b78dd939' as id),
     questionnaire AS (SELECT jsonb_array_elements(questionnaire) as questionConfig, id as "campaignId"
                       FROM "Campaigns"
                       WHERE id IN (SELECT id FROM campaignIds)),
     pivotQuestions AS (SELECT questionConfig ->> 'id' as "qId",
                           questionConfig -> 'config' ->> 'attributeName' as "attributeName",
                           "campaignId"
                    FROM questionnaire
                    WHERE questionConfig ->> 'componentKind' = 'MULTIPLE_CHOICE'
                      AND (questionConfig -> 'config' -> 'isDimension')::boolean IS TRUE
                      AND questionConfig -> 'config' ->> 'attributeName' = 'Speciality'),
     responses AS (
         SELECT count(*) AS "totalResponses", answers ->> "qId" as value, "attributeName"
         FROM "Responses"
                  INNER JOIN pivotQuestions on "Responses"."campaignId" = pivotQuestions."campaignId"
         WHERE "Responses"."campaignId" IN (SELECT id FROM campaignIds)
         and "Responses"."discarded" is false
           AND "Responses"."createdAt" <= '2024-04-01 00:00:00.000 +05:30'

         GROUP BY "attributeName", value
     ),
     allInvitesSent AS (
         SELECT distinct "ParticipantListMembers".id,
                         "attributeName",
                         "prefilledData" ->> "qId" as value,
                         success,
                         visited
         FROM "Schedules"
                  INNER JOIN "ParticipantLists" ON "Schedules"."participantListId" = "ParticipantLists".id
                  INNER JOIN "ParticipantListMembers"
                             ON "ParticipantLists".id = "ParticipantListMembers"."participantListId"
                  INNER JOIN pivotQuestions on "ParticipantLists"."campaignId" = pivotQuestions."campaignId"  
                  INNER JOIN "DistributionLogs" ON ("Schedules".id, "ParticipantListMembers".id) =
                                                   ("DistributionLogs"."scheduleId",
                                                    "DistributionLogs"."participantListMemberId")
         WHERE "ParticipantLists"."campaignId" IN (SELECT id FROM campaignIds)
           AND "sendTo" = 'ALL'
           AND "sentStatus" = 'SENT'
           AND "Schedules"."scheduleDateAndTime" <= '2024-04-01 00:00:00.000 +05:30'

     ),
     totalDelivered AS (
         SELECT "attributeName", COALESCE(value, '') as value, count(*) as "totalDeliveriesIrrespectiveOfReminders"
         FROM allInvitesSent

         GROUP BY "attributeName", value
     ),
     responseRate AS (SELECT round(("totalResponses" * 100 / coalesce(nullif("totalDeliveriesIrrespectiveOfReminders", 0), 1)),0) AS "responseRate",
                             coalesce("totalResponses", 0) as "totalResponses",
                             coalesce("totalDeliveriesIrrespectiveOfReminders", 0) as "totalDeliveriesIrrespectiveOfReminders",
                             coalesce(responses."attributeName", totalDelivered."attributeName") as "attributeName",
                             coalesce(NULLIF(responses.value, ''), NULLIF(totalDelivered.value, '')) AS value
                      FROM responses
                               FULL JOIN totalDelivered ON (responses."attributeName", responses.value) =
                                                            (totalDelivered."attributeName", totalDelivered.value))
,
          npsMetricConfigs AS ( SELECT * from "MetricConfigs" where "MetricConfigs"."campaignId" IN (SELECT id FROM campaignIds) AND type = 'NPS' ),
          questionsId AS (SELECT questionConfig ->> 'id' AS id, "campaignId"
          FROM questionnaire
          WHERE (questionConfig -> 'config' -> 'isNPSQuestion')::boolean = true
          ),
          segmentCountPerKeyword AS (
            SELECT "Responses".id, answers -> pivotQuestions."qId" ->> 0 AS value, "Responses"."campaignId",
            round(avg(NULLIF(answers ->> questionsId.id,'NA')::int), 1)::float AS "score"
            FROM "Responses"
            INNER JOIN questionsId ON "Responses"."campaignId" = questionsId."campaignId"
            INNER JOIN pivotQuestions ON "Responses"."campaignId" = pivotQuestions."campaignId"
            WHERE "Responses"."campaignId" IN (SELECT id FROM campaignIds) AND "Responses"."createdAt" <= '2024-04-01 00:00:00.000 +05:30'
            AND answers -> pivotQuestions."qId" ->> 0 IS NOT NULL
            AND answers ->> questionsId.id IS NOT NULL
            GROUP BY value,"createdAt","Responses".id
          ),
          countPerPivot AS (
            SELECT value,
            SUM(CASE WHEN (segmentCountPerKeyword.score)::int > npsMetricConfigs."promoterLowerRange" THEN 1 ELSE 0 END)::int as "promoters",
            SUM(CASE WHEN (segmentCountPerKeyword.score)::int > npsMetricConfigs."detractorUpperRange" AND (segmentCountPerKeyword.score)::int <= npsMetricConfigs."promoterLowerRange"  THEN 1 ELSE 0 END)::int as "passives",
            SUM(CASE WHEN (segmentCountPerKeyword.score)::int <= npsMetricConfigs."detractorUpperRange" THEN 1 ELSE 0 END)::int as "detractors"
            FROM segmentCountPerKeyword
            INNER JOIN npsMetricConfigs ON segmentCountPerKeyword."campaignId" = npsMetricConfigs."campaignId"
            GROUP BY value, npsMetricConfigs."promoterLowerRange", npsMetricConfigs."detractorUpperRange"
          )
          SELECT countPerPivot.value,(promoters + passives + detractors) AS all,
            round( CAST( (promoters - detractors)::float / (promoters + passives + detractors) * 100 AS numeric),1) AS "score",
            responseRate."totalResponses",
            responseRate."responseRate",
            responseRate."totalDeliveriesIrrespectiveOfReminders"
          FROM countPerPivot
          INNER JOIN responseRate ON ((responseRate.value)::jsonb
                               ->> 0 = countPerPivot.value)
          WHERE  (promoters + passives + detractors) <> 0
          ORDER BY score DESC







-------------------------------------------------------------------------
-------------------------------------------------------------------------





WITH campaignIds AS (SELECT child AS id
                            FROM "GroupedCampaignChildren"
                            WHERE parent = 'c8afefb2-ed74-43be-a722-f0b0b78dd939'
                            UNION
                            SELECT 'c8afefb2-ed74-43be-a722-f0b0b78dd939' as id),
            npsMetricConfig AS ( SELECT * from "MetricConfigs" where "MetricConfigs"."campaignId" IN (SELECT id FROM campaignIds) AND type = 'NPS' ),
            csatMetricConfig AS ( SELECT * from "MetricConfigs" where "MetricConfigs"."campaignId" IN (SELECT id FROM campaignIds) AND type = 'CSAT' ),
            record0 AS (SELECT
          CASE
            WHEN (TRUE IS TRUE) THEN
                CASE
                    WHEN (answers -> '6a62957e-ea0c-4388-b579-d1eec9514bd3')::int > csatMetricConfig."promoterLowerRange" THEN '["Promoter"]'::jsonb
                    WHEN (answers -> '6a62957e-ea0c-4388-b579-d1eec9514bd3')::int > csatMetricConfig."detractorUpperRange" THEN '["Passive"]'::jsonb
                    WHEN (answers -> '6a62957e-ea0c-4388-b579-d1eec9514bd3')::int <= csatMetricConfig."detractorUpperRange" THEN '["Detractor"]'::jsonb
                    END
              ELSE answers -> '6a62957e-ea0c-4388-b579-d1eec9514bd3'
            END AS "6a62957e-ea0c-4388-b579-d1eec9514bd3"
        ,
          CASE
            WHEN (FALSE IS TRUE) THEN
                CASE
                    WHEN (answers -> '1d9d7b65-706b-4c26-b8d0-a5805147b73f')::int > csatMetricConfig."promoterLowerRange" THEN '["Promoter"]'::jsonb
                    WHEN (answers -> '1d9d7b65-706b-4c26-b8d0-a5805147b73f')::int > csatMetricConfig."detractorUpperRange" THEN '["Passive"]'::jsonb
                    WHEN (answers -> '1d9d7b65-706b-4c26-b8d0-a5805147b73f')::int <= csatMetricConfig."detractorUpperRange" THEN '["Detractor"]'::jsonb
                    END
              ELSE answers -> '1d9d7b65-706b-4c26-b8d0-a5805147b73f'
            END AS "1d9d7b65-706b-4c26-b8d0-a5805147b73f"
         FROM "Responses"
                          INNER JOIN npsMetricConfig ON "Responses"."campaignId" = npsMetricConfig."campaignId"
                          INNER JOIN csatMetricConfig ON "Responses"."campaignId" = csatMetricConfig."campaignId"
                        WHERE "Responses"."campaignId" IN (SELECT id FROM campaignIds)
                        and "Responses"."discarded" is false
                        AND answers->'1d9d7b65-706b-4c26-b8d0-a5805147b73f' IS NOT NULL AND answers->>'1d9d7b65-706b-4c26-b8d0-a5805147b73f' <> ''
                         AND "Responses"."createdAt" >= '2023-04-01 00:00:00.000 +05:30'
                         AND "Responses"."createdAt" <= '2023-11-06 23:59:59.999 +05:30'
                        ),
            record1 AS (SELECT   jsonb_array_elements("6a62957e-ea0c-4388-b579-d1eec9514bd3") AS "6a62957e-ea0c-4388-b579-d1eec9514bd3" ,  "1d9d7b65-706b-4c26-b8d0-a5805147b73f" FROM record0 ), record2 AS (SELECT "6a62957e-ea0c-4388-b579-d1eec9514bd3" ,  jsonb_array_elements("1d9d7b65-706b-4c26-b8d0-a5805147b73f") AS "1d9d7b65-706b-4c26-b8d0-a5805147b73f"   FROM record1 )
        SELECT *, count(*)::int FROM record2

        GROUP BY "6a62957e-ea0c-4388-b579-d1eec9514bd3", "1d9d7b65-706b-4c26-b8d0-a5805147b73f"




   
-------------------------------------------------------------------------
-------------------------------------------------------------------------




WITH campaignIds AS (SELECT child AS id
                            FROM "GroupedCampaignChildren"
                            WHERE parent = 'c8afefb2-ed74-43be-a722-f0b0b78dd939'
                            UNION
                            SELECT 'c8afefb2-ed74-43be-a722-f0b0b78dd939' as id),
            npsMetricConfig AS ( SELECT * from "MetricConfigs" where "MetricConfigs"."campaignId" IN (SELECT id FROM campaignIds) AND type = 'NPS' ),
            csatMetricConfig AS ( SELECT * from "MetricConfigs" where "MetricConfigs"."campaignId" IN (SELECT id FROM campaignIds) AND type = 'CSAT' ),
            record0 AS (SELECT
          CASE
            WHEN (TRUE IS TRUE) THEN
                CASE
                    WHEN (answers -> '3197068f-7e0d-49c0-a557-ee3fcad67ac3')::int > csatMetricConfig."promoterLowerRange" THEN '["Promoter"]'::jsonb
                    WHEN (answers -> '3197068f-7e0d-49c0-a557-ee3fcad67ac3')::int > csatMetricConfig."detractorUpperRange" THEN '["Passive"]'::jsonb
                    WHEN (answers -> '3197068f-7e0d-49c0-a557-ee3fcad67ac3')::int <= csatMetricConfig."detractorUpperRange" THEN '["Detractor"]'::jsonb
                    END
              ELSE answers -> '3197068f-7e0d-49c0-a557-ee3fcad67ac3'
            END AS "3197068f-7e0d-49c0-a557-ee3fcad67ac3"
        ,
          CASE
            WHEN (FALSE IS TRUE) THEN
                CASE
                    WHEN (answers -> '948a3379-810f-4c6b-af49-29bf9a2444bb')::int > csatMetricConfig."promoterLowerRange" THEN '["Promoter"]'::jsonb
                    WHEN (answers -> '948a3379-810f-4c6b-af49-29bf9a2444bb')::int > csatMetricConfig."detractorUpperRange" THEN '["Passive"]'::jsonb
                    WHEN (answers -> '948a3379-810f-4c6b-af49-29bf9a2444bb')::int <= csatMetricConfig."detractorUpperRange" THEN '["Detractor"]'::jsonb
                    END
              ELSE answers -> '948a3379-810f-4c6b-af49-29bf9a2444bb'
            END AS "948a3379-810f-4c6b-af49-29bf9a2444bb"
         FROM "Responses"
                          INNER JOIN npsMetricConfig ON "Responses"."campaignId" = npsMetricConfig."campaignId"
                          INNER JOIN csatMetricConfig ON "Responses"."campaignId" = csatMetricConfig."campaignId"
                        WHERE "Responses"."campaignId" IN (SELECT id FROM campaignIds)
                        and "Responses"."discarded" is false
                        AND answers->'948a3379-810f-4c6b-af49-29bf9a2444bb' IS NOT NULL AND answers->>'948a3379-810f-4c6b-af49-29bf9a2444bb' <> ''
                         AND "Responses"."createdAt" >= '2023-04-01 00:00:00.000 +05:30'
                         AND "Responses"."createdAt" <= '2023-11-06 23:59:59.999 +05:30'
                        ),
            record1 AS (SELECT   jsonb_array_elements("3197068f-7e0d-49c0-a557-ee3fcad67ac3") AS "3197068f-7e0d-49c0-a557-ee3fcad67ac3" ,  "948a3379-810f-4c6b-af49-29bf9a2444bb" FROM record0 ), record2 AS (SELECT "3197068f-7e0d-49c0-a557-ee3fcad67ac3" ,  jsonb_array_elements("948a3379-810f-4c6b-af49-29bf9a2444bb") AS "948a3379-810f-4c6b-af49-29bf9a2444bb"   FROM record1 )
        SELECT *, count(*)::int FROM record2

        GROUP BY "3197068f-7e0d-49c0-a557-ee3fcad67ac3", "948a3379-810f-4c6b-af49-29bf9a2444bb"

----------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------


select * from "GroupedCampaignChildren" where "child" = '6bef29cd-2a0d-4326-baae-a7b58d7e635a';
select * from "DataPoints" where "campaignId" = 'b995140d-6ca3-4723-ae32-b71ffcb12f11';

SELECT dp.*, gcc.*
FROM "DataPoints" dp
JOIN "GroupedCampaignChildren" gcc
    ON dp."campaignId" = gcc."child" LIMIT 10000;


SELECT gcc.*
FROM "GroupedCampaignChildren" gcc
WHERE gcc."child" IN (
    SELECT dp."campaignId"
    FROM "DataPoints" dp
    JOIN "GroupedCampaignChildren" gcc2
        ON dp."campaignId" = gcc2."child"
   
);

select * from "Schedules" limit 100;

select * from "Schedules" where "campaignId" = 'c8afefb2-ed74-43be-a722-f0b0b78dd939' AND "sentStatus" = 'SENT' AND "campaignId" = 'c8afefb2-ed74-43be-a722-f0b0b78dd939' AND "sendTo" = 'ALL' AND "Schedules"."scheduleDateAndTime" <= '2024-04-01 00:00:00.000 +05:30'

select * from "ParticipantListMembers" where "participantId" = 'fc735689-8123-4dbd-bb5e-1b1410702aa1'
select * from "ParticipantListMembers" where "participantListId" = '828a574a-c730-4ee1-9b87-808c113ff142'
select  * from "Campaigns" where "organizationId" = '04511974-b6bf-4216-9223-321fedad5520' AND "id" = 'c8afefb2-ed74-43be-a722-f0b0b78dd939'



SELECT 
    "ParticipantLists".id AS "participantListId",
    COUNT(DISTINCT "Schedules".id) AS "scheduleCount"
FROM 
    "ParticipantLists"
INNER JOIN 
    "Schedules" ON "Schedules"."participantListId" = "ParticipantLists".id
GROUP BY 
    "ParticipantLists".id
HAVING 
    COUNT(DISTINCT "Schedules".id) > 1;


SELECT 
    "Schedules".id AS "scheduleId",
    COUNT(DISTINCT "ParticipantLists".id) AS "participantListCount"
FROM 
    "Schedules"
INNER JOIN 
    "ParticipantLists" ON "Schedules"."participantListId" = "ParticipantLists".id
GROUP BY 
    "Schedules".id
HAVING 
    COUNT(DISTINCT "ParticipantLists".id) > 0;


Select * from "Schedules" where "participantListId" = 'f6aa18d7-f1c9-44de-99fe-ea23b7dacbf5'


with allInvitesSent AS (select distinct 
				"ParticipantListMembers".id as pid,
				"Schedules".id as scID,
                         "prefilledData",
                         success,
                         visited
from "Schedules" 
	INNER JOIN "ParticipantLists" ON "Schedules"."participantListId" = "ParticipantLists".id
    INNER JOIN "ParticipantListMembers" ON "ParticipantLists".id = "ParticipantListMembers"."participantListId" 
	INNER JOIN "DistributionLogs" ON ("Schedules".id, "ParticipantListMembers".id) =
                                                   ("DistributionLogs"."scheduleId",
                                                    "DistributionLogs"."participantListMemberId")
where "sentStatus" = 'SENT' AND "ParticipantLists"."campaignId" = 'c8afefb2-ed74-43be-a722-f0b0b78dd939' AND "sendTo" = 'ALL' AND "Schedules"."scheduleDateAndTime" <= '2024-04-01 00:00:00.000 +05:30')
select * from allInvitesSent where "scid" = '86fe22d7-04dd-4284-b971-8be3d268c263'

--------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------