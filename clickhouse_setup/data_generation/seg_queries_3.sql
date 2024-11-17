-- 1. Basic Segment Analysis with NPS Scores  0.900sec
SELECT 
    seg.2 as brand,  -- segment_value from tuple
    count(DISTINCT ra.response_id) as total_responses,
    countIf(ra.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(ra.nps_score > mc.detractor_upper_range AND ra.nps_score < mc.promoter_lower_range) as passives,
    countIf(ra.nps_score <= mc.detractor_upper_range) as detractors,
    count(DISTINCT CASE WHEN ra.nps_score IS NOT NULL THEN ra.response_id END) as total_nps_responses,
    round((
        countIf(ra.nps_score >= mc.promoter_lower_range) - 
        countIf(ra.nps_score <= mc.detractor_upper_range)
    ) * 100.0 / nullIf(count(DISTINCT CASE WHEN ra.nps_score IS NOT NULL THEN ra.response_id END), 0), 1) as nps_score
FROM responses_array ra
ARRAY JOIN segment_data as seg
JOIN metric_configs mc ON ra.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE ra.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND ra.created_at BETWEEN '2023-01-01' AND '2024-03-31'
AND NOT ra.discarded
GROUP BY seg.2
ORDER BY nps_score DESC;

-- 2. JSON-based Analysis (Participant Demographics) 0.800sec
SELECT 
    seg.2 as brand,
    JSONExtractString(ra.participant_info, 'age_group') as age_group,
    count(DISTINCT ra.response_id) as total_responses,
    round(avg(ra.nps_score), 1) as avg_nps,
    countIf(ra.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(ra.nps_score <= mc.detractor_upper_range) as detractors
FROM responses_array ra
ARRAY JOIN segment_data as seg
JOIN metric_configs mc ON ra.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE ra.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND ra.created_at BETWEEN '2023-01-01' AND '2024-03-31'
AND NOT ra.discarded
GROUP BY seg.2, age_group
HAVING total_responses >= 10
ORDER BY brand, avg_nps DESC;

-- 3. Complex Multi-segment Filter Analysis 0.600msec
WITH filtered_responses AS (
    SELECT DISTINCT response_id
    FROM responses_array
    ARRAY JOIN segment_data as seg
    WHERE campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
    AND created_at BETWEEN '2023-01-01' AND '2024-03-31'
    AND (
        (seg.2 IN ('india', 'us'))
        OR (seg.2 = 'business')
    )
)
SELECT 
    seg.2 as brand,
    count(DISTINCT ra.response_id) as total_responses,
    countIf(ra.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(ra.nps_score > mc.detractor_upper_range AND ra.nps_score < mc.promoter_lower_range) as passives,
    countIf(ra.nps_score <= mc.detractor_upper_range) as detractors,
    round((
        countIf(ra.nps_score >= mc.promoter_lower_range) - 
        countIf(ra.nps_score <= mc.detractor_upper_range)
    ) * 100.0 / nullIf(count(DISTINCT ra.response_id), 0), 1) as nps_score
FROM responses_array ra
ARRAY JOIN segment_data as seg
JOIN metric_configs mc ON ra.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE ra.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND ra.created_at BETWEEN '2023-01-01' AND '2024-03-31'
AND ra.response_id IN (SELECT response_id FROM filtered_responses)
AND seg.2 IN ('indigo', 'airasia', 'spicejet', 'airindia', 'vistara')
GROUP BY seg.2
ORDER BY nps_score DESC;

-- 4. Trend Analysis by Month 0.450msec
SELECT 
    toStartOfMonth(ra.created_at) as month,
    seg.2 as brand,
    count(DISTINCT ra.response_id) as total_responses,
    countIf(ra.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(ra.nps_score > mc.detractor_upper_range AND ra.nps_score < mc.promoter_lower_range) as passives,
    countIf(ra.nps_score <= mc.detractor_upper_range) as detractors,
    round((
        countIf(ra.nps_score >= mc.promoter_lower_range) - 
        countIf(ra.nps_score <= mc.detractor_upper_range)
    ) * 100.0 / nullIf(count(DISTINCT ra.response_id), 0), 1) as nps_score
FROM responses_array ra
ARRAY JOIN segment_data as seg
JOIN metric_configs mc ON ra.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE ra.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND ra.created_at BETWEEN '2023-01-01' AND '2024-03-31'
GROUP BY 
    month,
    seg.2
ORDER BY 
    month,
    nps_score DESC;


-- SELECT COUNT(*)
-- FROM campaigns

-- Query id: 4d683ec5-5ce3-44bf-a5fc-e01fdcf7c838

--    ┌─COUNT()─┐
-- 1. │      15 │
--    └─────────┘

-- 1 row in set. Elapsed: 0.005 sec. 

-- dc2c8dffbcf8 :) select COUNT(*) from distribution_facts;

-- SELECT COUNT(*)
-- FROM distribution_facts

-- Query id: 81346b39-304b-4a4c-82fb-0f5e67965488

--    ┌─COUNT()─┐
-- 1. │ 9000000 │ -- 9.00 million
--    └─────────┘

-- 1 row in set. Elapsed: 0.038 sec. 

-- dc2c8dffbcf8 :) select COUNT(*) from metric_configs;

-- SELECT COUNT(*)
-- FROM metric_configs

-- Query id: 8b0ad855-15e0-4cb7-aafc-c9fcbc0c7fed

--    ┌─COUNT()─┐
-- 1. │      15 │
--    └─────────┘

-- 1 row in set. Elapsed: 0.005 sec. 

-- dc2c8dffbcf8 :) select COUNT(*) from question_segment_mapping;

-- SELECT COUNT(*)
-- FROM question_segment_mapping

-- Query id: d86c8145-eacc-481f-b90b-ec3e52418c4a

--    ┌─COUNT()─┐
-- 1. │       4 │
--    └─────────┘

-- 1 row in set. Elapsed: 0.013 sec. 

-- dc2c8dffbcf8 :) select COUNT(*) from responses_array;

-- SELECT COUNT(*)
-- FROM responses_array

-- Query id: 545341ea-3359-4759-9885-66dd66512486

--    ┌─COUNT()─┐
-- 1. │ 9000000 │ -- 9.00 million
--    └─────────┘

-- 1 row in set. Elapsed: 0.004 sec. 
