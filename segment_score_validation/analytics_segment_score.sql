CREATE TABLE AggregatedResponses (
    response_id UUID,                             -- Unique Response ID
    campaign_id UUID,                             -- Campaign ID of the response
    parent_campaign_id UUID,                      -- Parent Campaign ID to support hierarchy
    participant_id UUID,                          -- Participant ID
    participant_info JSON,                        -- JSON for additional participant info
    answers JSON,                                 -- JSON-formatted response answers
    created_at DateTime,                          -- Creation timestamp
    updated_at DateTime,                          -- Update timestamp
    submission_type String,                       -- Submission type
    participant_info_sensitive String,            -- Sensitive participant info
    assisted_by String,                           -- Assisted by
    calculated_answers_data JSON,                 -- JSON-formatted calculated answers data
    calculated_response_data JSON,                -- JSON-formatted calculated response data
    discarded UInt8 DEFAULT 0,                    -- Whether response is discarded
    participant_list_member_id UUID,              -- Participant list member ID

    -- Additional columns for analytics
    promoter UInt8,                               -- 1 if promoter, 0 otherwise
    passive UInt8,                                -- 1 if passive, 0 otherwise
    detractor UInt8,                              -- 1 if detractor, 0 otherwise
    segment String,                                -- Stores segment name
    segment_value String                         -- Stores extracted values for pivot questions i.e.segment value
) ENGINE = MergeTree()
ORDER BY (parent_campaign_id, campaign_id, created_at);

----------

CREATE TABLE Computed_ParticipantListMembers (
    participant_list_id UUID,                     -- Original: participantListId
    participant_id UUID,                          -- Original: participantId
    prefilled_data JSON,                          -- Original: prefilledData in JSON format
    created_at DateTime,                          -- Original: createdAt
    updated_at DateTime,                          -- Original: updatedAt
    token String,                                 -- Original: token
    is_response_submitted UInt8 DEFAULT 0,        -- Original: isResponseSubmitted
    response_id UUID,                             -- Original: responseId
    token_status String,                          -- Original: tokenStatus
    id UUID,                                      -- Original: ID
    visited UInt8 DEFAULT 0,                      -- Original: visited
    discarded UInt8 DEFAULT 0,                    -- Original: discarded
    
    -- Additional fields
    segment String,                               -- New: Stores segment name
    segment_value String                          -- New: Stores prefilled segment value
) ENGINE = MergeTree()
ORDER BY (participant_list_id, participant_id, created_at);

----------

CREATE TABLE Computed_Schedules (
    id UUID,                                      -- Original: schedule ID
    campaign_id UUID,                             -- Original: campaignId
    participant_list_id UUID,                     -- Original: participantListId
    template_id UUID,                             -- Original: templateId
    schedule_date_and_time DateTime,              -- Original: scheduleDateAndTime
    sent_status String,                           -- Original: sentStatus
    successful Int32 DEFAULT 0,                   -- Original: successful
    delivery_failures Int32 DEFAULT 0,            -- Original: deliveryFailures
    created_at DateTime,                          -- Original: createdAt
    updated_at DateTime,                          -- Original: updatedAt
    send_to String,                               -- Original: sendTo
    provider String,                              -- Original: provider
    is_auto_reminder UInt8 DEFAULT 0,             -- Original: isAutoReminder
    schedule_rule_set_id UUID,                    -- Original: scheduleRuleSetId
    process_start_time DateTime,                  -- Original: processStartTime
    process_end_time DateTime                     -- Original: processEndTime
) ENGINE = MergeTree()
ORDER BY (campaign_id, participant_list_id, schedule_date_and_time);

-----------


-----------------
-- Query for Segment Score Analytics 
------------------------------------------------------------------------------------

WITH response_stats AS (
    SELECT 
        segment_value AS value,                                 -- Equivalent to `pivot_question_answer_value`
        SUM(promoter) AS promoters,
        SUM(passive) AS passives,
        SUM(detractor) AS detractors,
        COUNT(*) AS totalResponses                             -- Total responses for each value
    FROM AggregatedResponses
    WHERE parent_campaign_id = 'c8afefb2-ed74-43be-a722-f0b0b78dd939'
      AND discarded = 0                                        -- Exclude discarded responses
      AND created_at <= '2024-04-01 00:00:00'                  -- Date range filter
      AND segment = 'specified_segment_name'                   -- Filter for specified segment name
    GROUP BY segment_value
),

filtered_schedules AS (
    SELECT id AS schedule_id
    FROM Computed_Schedules
    WHERE campaign_id IN (SELECT campaign_id FROM AggregatedResponses WHERE parent_campaign_id = 'c8afefb2-ed74-43be-a722-f0b0b78dd939')
      AND send_to = 'ALL'
      AND sent_status = 'SENT'
      AND schedule_date_and_time <= '2024-04-01 00:00:00'      -- Date range filter for schedules
),

all_invites_sent AS (
    SELECT DISTINCT
        Computed_ParticipantListMembers.id AS participant_id,
        Computed_ParticipantListMembers.segment AS attribute_name,
        COALESCE(Computed_ParticipantListMembers.segment_value, '') AS value
    FROM filtered_schedules
    INNER JOIN Computed_ParticipantListMembers 
        ON Computed_ParticipantListMembers.participant_list_id = filtered_schedules.participant_list_id
    WHERE Computed_ParticipantListMembers.segment = 'specified_segment_name' -- Filter by specified segment name
),

total_delivered AS (
    SELECT attribute_name, COALESCE(value, '') AS value, COUNT(*) AS totalDeliveriesIrrespectiveOfReminders
    FROM all_invites_sent
    GROUP BY attribute_name, value
),

response_rate AS (
    SELECT 
        COALESCE(response_stats.value, total_delivered.value) AS value,
        COALESCE(response_stats.totalResponses, 0) AS totalResponses,
        COALESCE(total_delivered.totalDeliveriesIrrespectiveOfReminders, 1) AS totalDeliveriesIrrespectiveOfReminders,
        ROUND((COALESCE(response_stats.totalResponses, 0) * 100 / NULLIF(COALESCE(total_delivered.totalDeliveriesIrrespectiveOfReminders, 1), 0)), 0) AS responseRate,
        COALESCE(response_stats.promoters, 0) AS promoters,
        COALESCE(response_stats.passives, 0) AS passives,
        COALESCE(response_stats.detractors, 0) AS detractors,
        ROUND((COALESCE(response_stats.promoters, 0) - COALESCE(response_stats.detractors, 0)) / NULLIF((COALESCE(response_stats.promoters, 0) + COALESCE(response_stats.passives, 0) + COALESCE(response_stats.detractors, 0)), 0) * 100, 1) AS score
    FROM response_stats
    FULL JOIN total_delivered ON response_stats.value = total_delivered.value
)

SELECT 
    value,
    totalResponses AS all,
    score,
    responseRate,
    totalDeliveriesIrrespectiveOfReminders,
    promoters,
    passives,
    detractors
FROM response_rate
WHERE totalResponses > 0
ORDER BY score DESC;


-----------------
-- Query for Segment Score Analytics 
------------------------------------------------------------------------------------


WITH response_stats AS (
    SELECT 
        segment_value AS value,                                 -- Equivalent to `pivot_question_answer_value`
        SUM(promoter) AS promoters,
        SUM(passive) AS passives,
        SUM(detractor) AS detractors,
        COUNT(*) AS totalResponses                             -- Total responses for each value
    FROM AggregatedResponses
    WHERE parent_campaign_id = 'c8afefb2-ed74-43be-a722-f0b0b78dd939'
      AND discarded = 0                                        -- Exclude discarded responses
      AND created_at <= '2024-04-01 00:00:00'                  -- Date range filter
      AND segment = 'specified_segment_name'                   -- Filter for specified segment name
    GROUP BY segment_value
),

filtered_schedules AS (
    SELECT id AS schedule_id
    FROM Computed_Schedules
    WHERE campaign_id IN (SELECT campaign_id FROM AggregatedResponses WHERE parent_campaign_id = 'c8afefb2-ed74-43be-a722-f0b0b78dd939')
      AND send_to = 'ALL'
      AND sent_status = 'SENT'
      AND schedule_date_and_time <= '2024-04-01 00:00:00'      -- Date range filter for schedules
),

all_invites_sent AS (
    SELECT DISTINCT
        Computed_ParticipantListMembers.id AS participant_id,
        Computed_ParticipantListMembers.segment AS attribute_name,
        Computed_ParticipantListMembers.segment_value AS value
    FROM filtered_schedules
    INNER JOIN Computed_ParticipantListMembers 
        ON Computed_ParticipantListMembers.participant_list_id = filtered_schedules.participant_list_id
    WHERE Computed_ParticipantListMembers.segment = 'specified_segment_name' -- Filter by specified segment name
),

total_delivered AS (
    SELECT attribute_name, COALESCE(value, '') AS value, COUNT(*) AS totalDeliveriesIrrespectiveOfReminders
    FROM all_invites_sent
    GROUP BY attribute_name, value
),

response_rate AS (
    SELECT 
        response_stats.value,
        response_stats.totalResponses,
        total_delivered.totalDeliveriesIrrespectiveOfReminders,
        ROUND((response_stats.totalResponses * 100 / NULLIF(total_delivered.totalDeliveriesIrrespectiveOfReminders, 0)), 0) AS responseRate,
        response_stats.promoters,
        response_stats.passives,
        response_stats.detractors,
        ROUND((response_stats.promoters - response_stats.detractors) / NULLIF((response_stats.promoters + response_stats.passives + response_stats.detractors), 0) * 100, 1) AS score
    FROM response_stats
    LEFT JOIN total_delivered ON response_stats.value = total_delivered.value
)

SELECT 
    value,
    totalResponses AS all,
    score,
    responseRate,
    totalDeliveriesIrrespectiveOfReminders,
    promoters,
    passives,
    detractors
FROM response_rate
WHERE totalResponses > 0
ORDER BY score DESC;















-----------------------------------------------------------------------------------------------------
-- Query for Segment Score Analytics 

SELECT 
    segment_value AS segment_value,           -- Accessing pivot question value directly
    SUM(promoter) AS promoters, 
    SUM(passive) AS passives, 
    SUM(detractor) AS detractors,
    toYear(created_at) AS year,                           -- Deriving year from created_at
    toWeek(created_at) AS week,                           -- Deriving week from created_at
    (promoters + passives + detractors) AS total_responses,
    ROUND((promoters - detractors) / (promoters + passives + detractors) * 100, 1) AS net_promoter_score
FROM AggregatedResponses
WHERE parent_campaign_id = 'c8afefb2-ed74-43be-a722-f0b0b78dd939'
  AND created_at BETWEEN '2023-04-01 00:00:00' AND '2023-11-06 23:59:59'  -- Date range filter
  AND pivot_question_answer_value IS NOT NULL                             -- Ensuring pivot question value is present
GROUP BY pivot_question_answer_value, year, week
HAVING total_responses > 0
ORDER BY year, week;


-------------------


SELECT 
    pivot_question_answer_value AS value,  -- Pivot question answer (e.g., Specialty)
    segment,                               -- Pre-stored segment value
    COUNT(*) AS segment_count              -- Count of responses per segment
FROM AggregatedResponses
WHERE parent_campaign_id = 'c8afefb2-ed74-43be-a722-f0b0b78dd939'
  AND created_at BETWEEN '2023-04-01 00:00:00' AND '2023-11-06 23:59:59'
  AND pivot_question_answer_value IS NOT NULL
GROUP BY value, segment
ORDER BY value, segment;
