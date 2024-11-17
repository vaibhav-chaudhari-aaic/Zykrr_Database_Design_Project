-- 1. Basic Segment Analysis with NPS Scores 0.800sec
SELECT 
    rd.segment_value as brand,
    count(DISTINCT rd.response_id) as total_responses,
    countIf(rd.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(rd.nps_score > mc.detractor_upper_range AND rd.nps_score < mc.promoter_lower_range) as passives,
    countIf(rd.nps_score <= mc.detractor_upper_range) as detractors,
    count(DISTINCT CASE WHEN rd.nps_score IS NOT NULL THEN rd.response_id END) as total_nps_responses,
    round((
        countIf(rd.nps_score >= mc.promoter_lower_range) - 
        countIf(rd.nps_score <= mc.detractor_upper_range)
    ) * 100.0 / nullIf(count(DISTINCT CASE WHEN rd.nps_score IS NOT NULL THEN rd.response_id END), 0), 1) as nps_score
FROM responses_denormalized rd
JOIN metric_configs mc ON rd.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE rd.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND rd.created_at BETWEEN '2023-01-01' AND '2024-03-31'
AND NOT rd.discarded
GROUP BY rd.segment_value
ORDER BY nps_score DESC;

-- 2. JSON-based Analysis (Participant Demographics) 0.600sec
SELECT 
    rd.segment_value as brand,
    JSONExtractString(rd.participant_info, 'age_group') as age_group,
    count(DISTINCT rd.response_id) as total_responses,
    round(avg(rd.nps_score), 1) as avg_nps,
    countIf(rd.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(rd.nps_score <= mc.detractor_upper_range) as detractors
FROM responses_denormalized rd
JOIN metric_configs mc ON rd.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE rd.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND rd.created_at BETWEEN '2023-01-01' AND '2024-03-31'
AND NOT rd.discarded
GROUP BY rd.segment_value, age_group
HAVING total_responses >= 10
ORDER BY brand, avg_nps DESC;

-- 3. Complex Multi-segment Filter Analysis 0.400sec
WITH filtered_responses AS (
    SELECT DISTINCT response_id
    FROM responses_denormalized
    WHERE campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
    AND created_at BETWEEN '2023-01-01' AND '2024-03-31'
    AND (
        (segment_value IN ('india', 'us'))
        OR (segment_value = 'business')
    )
)
SELECT 
    rd.segment_value as brand,
    count(DISTINCT rd.response_id) as total_responses,
    countIf(rd.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(rd.nps_score > mc.detractor_upper_range AND rd.nps_score < mc.promoter_lower_range) as passives,
    countIf(rd.nps_score <= mc.detractor_upper_range) as detractors,
    round((
        countIf(rd.nps_score >= mc.promoter_lower_range) - 
        countIf(rd.nps_score <= mc.detractor_upper_range)
    ) * 100.0 / nullIf(count(DISTINCT rd.response_id), 0), 1) as nps_score
FROM responses_denormalized rd
JOIN metric_configs mc ON rd.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE rd.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND rd.created_at BETWEEN '2023-01-01' AND '2024-03-31'
AND rd.response_id IN (SELECT response_id FROM filtered_responses)
AND rd.segment_value IN ('indigo', 'airasia', 'spicejet', 'airindia', 'vistara')
GROUP BY rd.segment_value
ORDER BY nps_score DESC;

-- 4. Trend Analysis by Month 0.350sec
SELECT 
    toStartOfMonth(rd.created_at) as month,
    rd.segment_value,
    count(DISTINCT rd.response_id) as total_responses,
    countIf(rd.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(rd.nps_score > mc.detractor_upper_range AND rd.nps_score < mc.promoter_lower_range) as passives,
    countIf(rd.nps_score <= mc.detractor_upper_range) as detractors,
    round((
        countIf(rd.nps_score >= mc.promoter_lower_range) - 
        countIf(rd.nps_score <= mc.detractor_upper_range)
    ) * 100.0 / nullIf(count(DISTINCT rd.response_id), 0), 1) as nps_score
FROM responses_denormalized rd
JOIN metric_configs mc ON rd.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE rd.campaign_id = (SELECT campaign_id FROM campaigns LIMIT 1)
AND rd.created_at BETWEEN '2023-01-01' AND '2024-03-31'
GROUP BY 
    month,
    rd.segment_value
ORDER BY 
    month,
    nps_score DESC;



-- SELECT COUNT(*)
-- FROM campaigns


--    ┌─COUNT()─┐
-- 1. │      15 │
--    └─────────┘


-- SELECT COUNT(*)
-- FROM distribution_facts



--    ┌─COUNT()─┐
-- 1. │ 9000000 │ -- 9.00 million
--    └─────────┘


-- SELECT COUNT(*)
-- FROM metric_configs



--    ┌─COUNT()─┐
-- 1. │      15 │
--    └─────────┘


-- SELECT COUNT(*)
-- FROM question_segment_mapping



--    ┌─COUNT()─┐
-- 1. │       4 │
--    └─────────┘



-- SELECT COUNT(*)
-- FROM responses_denormalized



--    ┌──COUNT()─┐
-- 1. │ 35899045 │ -- 35.90 million
--    └──────────┘



