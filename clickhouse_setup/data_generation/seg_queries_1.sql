-- 1. Basic Segment Analysis with NPS Scores 3.7sec
SELECT 
    srf.segment_value as brand,
    count(DISTINCT rf.response_id) as total_responses,
    countIf(rf.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(rf.nps_score > mc.detractor_upper_range AND rf.nps_score < mc.promoter_lower_range) as passives,
    countIf(rf.nps_score <= mc.detractor_upper_range) as detractors,
    count(DISTINCT CASE WHEN rf.nps_score IS NOT NULL THEN rf.response_id END) as total_nps_responses,
    round((
        countIf(rf.nps_score >= mc.promoter_lower_range) - 
        countIf(rf.nps_score <= mc.detractor_upper_range)
    ) * 100.0 / nullIf(count(DISTINCT CASE WHEN rf.nps_score IS NOT NULL THEN rf.response_id END), 0), 1) as nps_score
FROM segment_response_facts srf
JOIN response_facts rf ON srf.response_id = rf.response_id
JOIN metric_configs mc ON rf.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE srf.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND srf.created_at BETWEEN '2023-01-01' AND '2024-03-31'
AND NOT rf.discarded
GROUP BY srf.segment_value
ORDER BY nps_score DESC;

-- 2. JSON-based Analysis (Participant Demographics) 5.1sec
SELECT 
    srf.segment_value as brand,
    JSONExtractString(rf.participant_info, 'age_group') as age_group,
    count(DISTINCT rf.response_id) as total_responses,
    round(avg(rf.nps_score), 1) as avg_nps,
    countIf(rf.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(rf.nps_score <= mc.detractor_upper_range) as detractors
FROM segment_response_facts srf
JOIN response_facts rf ON srf.response_id = rf.response_id
JOIN metric_configs mc ON rf.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE srf.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND srf.created_at BETWEEN '2023-01-01' AND '2024-03-31'
AND NOT rf.discarded
GROUP BY srf.segment_value, age_group
HAVING total_responses >= 10
ORDER BY brand, avg_nps DESC;

-- 3. Complex Multi-segment Filter Analysis 8.4sec
SELECT 
    srf2.segment_value as brand,
    count(DISTINCT rf.response_id) as total_responses,
    countIf(rf.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(rf.nps_score > mc.detractor_upper_range AND rf.nps_score < mc.promoter_lower_range) as passives,
    countIf(rf.nps_score <= mc.detractor_upper_range) as detractors,
    round((
        countIf(rf.nps_score >= mc.promoter_lower_range) - 
        countIf(rf.nps_score <= mc.detractor_upper_range)
    ) * 100.0 / nullIf(count(DISTINCT rf.response_id), 0), 1) as nps_score
FROM segment_response_facts srf1
JOIN segment_response_facts srf2 ON srf1.response_id = srf2.response_id
JOIN response_facts rf ON srf1.response_id = rf.response_id
JOIN metric_configs mc ON rf.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE srf1.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND srf1.created_at BETWEEN '2023-01-01' AND '2024-03-31'
AND ((srf1.segment_value IN ('india', 'us')) OR (srf1.segment_value = 'business'))
AND srf2.segment_value IN ('indigo', 'airasia', 'spicejet', 'airindia', 'vistara')
GROUP BY srf2.segment_value
ORDER BY nps_score DESC;

-- 4. Trend Analysis by Month 3.05sec
SELECT 
    toStartOfMonth(srf.created_at) as month,
    srf.segment_value,
    count(DISTINCT rf.response_id) as total_responses,
    countIf(rf.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(rf.nps_score > mc.detractor_upper_range AND rf.nps_score < mc.promoter_lower_range) as passives,
    countIf(rf.nps_score <= mc.detractor_upper_range) as detractors,
    round((
        countIf(rf.nps_score >= mc.promoter_lower_range) - 
        countIf(rf.nps_score <= mc.detractor_upper_range)
    ) * 100.0 / nullIf(count(DISTINCT rf.response_id), 0), 1) as nps_score
FROM segment_response_facts srf
JOIN response_facts rf ON srf.response_id = rf.response_id
JOIN metric_configs mc ON rf.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE srf.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND srf.created_at BETWEEN '2023-01-01' AND '2024-03-31'
GROUP BY 
    month,
    srf.segment_value
ORDER BY 
    month,
    nps_score DESC;


-- dc2c8dffbcf8 :) select COUNT(*) from campaigns;

-- SELECT COUNT(*)
-- FROM campaigns

-- Query id: 1edf1b9c-26c9-486a-9856-f71f8452e87e

--    ┌─COUNT()─┐
-- 1. │      15 │
--    └─────────┘

-- 1 row in set. Elapsed: 0.012 sec. 

-- dc2c8dffbcf8 :) select COUNT(*) from distribution_facts;

-- SELECT COUNT(*)
-- FROM distribution_facts

-- Query id: 17ed71eb-a4c9-4a80-b8db-029a07a52149

--    ┌─COUNT()─┐
-- 1. │ 9000000 │ -- 9.00 million
--    └─────────┘

-- 1 row in set. Elapsed: 0.013 sec. 

-- dc2c8dffbcf8 :) select COUNT(*) from metric_configs;

-- SELECT COUNT(*)
-- FROM metric_configs

-- Query id: 271ca050-ddb0-4902-8e99-d0688933fbb7

--    ┌─COUNT()─┐
-- 1. │      15 │
--    └─────────┘

-- 1 row in set. Elapsed: 0.005 sec. 

-- dc2c8dffbcf8 :) select COUNT(*) from question_segment_mapping;

-- SELECT COUNT(*)
-- FROM question_segment_mapping

-- Query id: db4257f0-8446-4836-871c-5b0956d46d1d

--    ┌─COUNT()─┐
-- 1. │       4 │
--    └─────────┘

-- 1 row in set. Elapsed: 0.004 sec. 

-- dc2c8dffbcf8 :) select COUNT(*) from response_facts;

-- SELECT COUNT(*)
-- FROM response_facts

-- Query id: 0b650add-769a-426d-bfe5-eb6cf87e1acf

--    ┌─COUNT()─┐
-- 1. │ 9000000 │ -- 9.00 million
--    └─────────┘

-- 1 row in set. Elapsed: 0.005 sec. 

-- dc2c8dffbcf8 :) select COUNT(*) from segment_response_facts;

-- SELECT COUNT(*)
-- FROM segment_response_facts

-- Query id: e9f2b7a8-b4ce-4a67-8db3-87cecf69c8a8

--    ┌──COUNT()─┐
-- 1. │ 36000000 │ -- 36.00 million
--    └──────────┘

-- 1 row in set. Elapsed: 0.004 sec. 