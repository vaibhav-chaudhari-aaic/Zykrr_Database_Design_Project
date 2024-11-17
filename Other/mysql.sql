-- I have my databas in postgresql I'm using same database for analytics purpose but I want to improve my performance for analytcis so I have thought hybrid apporach like my main db which stores application data in postgresql and for analytics purpose we can use clickhouse and put only those data which is required for analytics in proper strucutre to get better performance.

-- Below is one use case of analytics and given query which I currently run on old db refer it.


====================================================
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


==============
-- Below are entities used in above query

CREATE TABLE IF NOT EXISTS public."Responses"
(
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    "campaignId" uuid NOT NULL,
    answers jsonb NOT NULL,
    "participantId" uuid,
    "participantInfo" jsonb,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now(),
    "submissionType" "enum_Responses_submissionType" DEFAULT 'SUBMIT'::"enum_Responses_submissionType",
    "participantInfoSensitive" bytea,
    "assistedBy" character varying(255) COLLATE pg_catalog."default",
    "calculatedAnswersData" jsonb DEFAULT '{}'::jsonb,
    "calculatedResponseData" jsonb DEFAULT '{}'::jsonb,
    discarded boolean NOT NULL DEFAULT false,
    "participantListMemberId" uuid,
    CONSTRAINT "Responses_pkey" PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS public."GroupedCampaignChildren"
(
    parent uuid NOT NULL,
    child uuid NOT NULL,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public."Campaigns"
(
    id uuid NOT NULL,
    "organizationId" uuid NOT NULL,
    name character varying(255) COLLATE pg_catalog."default" NOT NULL,
    description character varying(255) COLLATE pg_catalog."default",
    questionnaire jsonb,
    industry character varying(255) COLLATE pg_catalog."default",
    logo character varying(255) COLLATE pg_catalog."default",
    status character varying(255) COLLATE pg_catalog."default" NOT NULL DEFAULT 'TEST'::character varying,
    thankyoupagesettings jsonb DEFAULT '[]'::jsonb,
    "filtersVisiblity" jsonb DEFAULT '{}'::jsonb,
    "allowPartialSubmit" boolean DEFAULT false,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now(),
    "tokenExpiryInHours" integer,
    "benchmarkIndustry" uuid,
    "enableSendingNotifications" boolean DEFAULT false,
    "surveyView" "enum_Campaigns_surveyView" DEFAULT 'NORMAL'::"enum_Campaigns_surveyView",
    languages character varying(255)[] COLLATE pg_catalog."default" NOT NULL DEFAULT ARRAY['English'::character varying(255)],
    translations jsonb NOT NULL DEFAULT '{}'::jsonb,
    type "enum_Campaigns_type" NOT NULL DEFAULT 'GENERAL'::"enum_Campaigns_type",
    "isTextAnalysisTranslationEnabled" boolean NOT NULL DEFAULT false,
    "autoStartOverInSec" integer,
    "campaignTitle" character varying(255) COLLATE pg_catalog."default",
    "dataRetentionPeriodInDays" integer,
    "googleAnalyticsCode" character varying(20) COLLATE pg_catalog."default",
    "thankyouPhotoLink" character varying(512) COLLATE pg_catalog."default",
    "thankyouPhoto" character varying(512) COLLATE pg_catalog."default",
    "enableHuddleModule" boolean NOT NULL DEFAULT false,
    "enableCallerModule" boolean NOT NULL DEFAULT false,
    "quotaFill" jsonb DEFAULT '{}'::jsonb,
    "isNonRespondedFieldsHiddenInResponsePopup" boolean NOT NULL DEFAULT false,
    "escalationNameSingular" character varying(255) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    "escalationNamePlural" character varying(255) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    "questionsVisible" jsonb DEFAULT '{}'::jsonb,
    "isDistributionEnable" boolean NOT NULL DEFAULT false,
    "isScheduleEnable" boolean NOT NULL DEFAULT false,
    "enablePreCalculation" boolean NOT NULL DEFAULT false,
    "campaignStartDate" timestamp with time zone,
    "partialSubmitSettings" jsonb DEFAULT '{}'::jsonb,
    CONSTRAINT "Campaigns_pkey" PRIMARY KEY (id),
    CONSTRAINT "Campaigns_name_organizationId_key" UNIQUE (name, "organizationId")
);

CREATE TABLE IF NOT EXISTS public."MetricConfigs"
(
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    "campaignId" uuid NOT NULL,
    "rangeStart" numeric(10, 2) NOT NULL,
    "rangeEnd" numeric(10, 2) NOT NULL,
    "detractorUpperRange" numeric(10, 2) NOT NULL,
    "promoterLowerRange" numeric(10, 2) NOT NULL,
    type "enum_MetricConfigs_type" NOT NULL,
    "promoterLabel" character varying(1000) COLLATE pg_catalog."default" DEFAULT 'Promoter'::character varying,
    "detractorLabel" character varying(1000) COLLATE pg_catalog."default" DEFAULT 'Detractor'::character varying,
    "passiveLabel" character varying(1000) COLLATE pg_catalog."default" DEFAULT 'Passive'::character varying,
    "metricLabel" character varying(1000) COLLATE pg_catalog."default" NOT NULL,
    "createdAt" timestamp with time zone NOT NULL DEFAULT now(),
    "updatedAt" timestamp with time zone NOT NULL DEFAULT now(),
    "metricLabelShort" character varying(1000) COLLATE pg_catalog."default",
    CONSTRAINT "MetricConfigs_pkey" PRIMARY KEY (id),
    CONSTRAINT "MetricConfigs_key" UNIQUE ("campaignId", type)
);


==========================


-- Below is Table structure I have think that can in clickhouse for analytics purpose.

-- Approach -1

-- Campaign level in single table store npsMetrics for each camapign,store nps questions for each campaign ,pivoteQuestions because things are not changes frequesntly over date range

-- In other table store responses as in given query or old strucutre and do similar kind of query to same results as above query us giving

-- The reason to store campaign level things in single table is in old postgres strucutre for every time query runs we need fetch these npsm-etrics,npsquestions,pivotequestions for that campaign but this fields are not frequently changes so we can put it in single table and frequently date range depedent resposnses we follow similar strutucure.



-- Approach -2 

-- In old query we can see that for getting promoter,detractor,passive we are checking for each response whether it is under given npsMetrics and setting 1 or 0 to that response as per condition validated so in new clickhouse strucutre we can do like

-- We can have one table which stores each response with more columns which have promotor ,detractor and passive and 0 or 1 values in each columns as per npsmetrics conditions check so that due this approach we are bypassing for each time checking whether this reposne is promoter,detractor or passive.

-- And based on this single we can get required output as old query gives.


-- Now your task is give proper strucutres old, and new strucutre to store in clickhousr for both approaches and query we can run on new strucutres to get same results



