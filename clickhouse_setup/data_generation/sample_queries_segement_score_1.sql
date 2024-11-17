
-- 1. Basic Segment Analysis with NPS Scores and Response Rate
WITH response_metrics AS (
    SELECT 
        srf.segment_value,
        count(DISTINCT rf.response_id) as total_responses,
        countIf(rf.nps_score >= mc.promoter_lower_range) as promoters,
        countIf(rf.nps_score > mc.detractor_upper_range AND rf.nps_score < mc.promoter_lower_range) as passives,
        countIf(rf.nps_score <= mc.detractor_upper_range) as detractors,
        count(DISTINCT CASE WHEN rf.nps_score IS NOT NULL THEN rf.response_id END) as total_nps_responses
    FROM segment_response_facts srf
    JOIN response_facts rf ON srf.response_id = rf.response_id
    JOIN metric_configs mc ON rf.campaign_id = mc.campaign_id AND mc.type = 'NPS'
    WHERE srf.campaign_id = toUUID('30a1d1a2-0d3f-4673-81cb-ba98530220cf') 

    -- WHERE srf.campaign_id = '{{your_campaign_id}}'
    AND srf.created_at BETWEEN '2023-01-01' AND '2024-03-31'
    AND srf.segment_name = 'brand'
    AND NOT rf.discarded
    GROUP BY srf.segment_value
),
distribution_metrics AS (
    SELECT count(DISTINCT CASE WHEN is_delivered = 1 THEN participant_list_member_id END) as total_delivered
    FROM distribution_facts
    WHERE campaign_id = toUUID('30a1d1a2-0d3f-4673-81cb-ba98530220cf')
)
SELECT 
    rm.segment_value as brand,
    rm.total_responses,
    rm.promoters,
    rm.passives,
    rm.detractors,
    round((rm.promoters - rm.detractors) * 100.0 / nullIf(rm.total_nps_responses, 0), 1) as nps_score,
    round(rm.total_responses * 100.0 / nullIf(dm.total_delivered, 0), 1) as response_rate
FROM response_metrics rm
CROSS JOIN distribution_metrics dm
ORDER BY nps_score DESC;

-- 2. Multi-segment Filter Analysis
WITH filtered_responses AS (
    SELECT DISTINCT srf.response_id
    FROM segment_response_facts srf
    WHERE srf.campaign_id = toUUID('32265e50-5a64-4acb-8ad1-5279d5c5cc2a') 
    AND srf.created_at BETWEEN '2023-01-01' AND '2024-03-31'
    AND (
        (srf.segment_name = 'country' AND srf.segment_value IN ('india', 'us'))
        OR (srf.segment_name = 'class' AND srf.segment_value = 'business')
    )
)
SELECT 
    srf.segment_value as brand,
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
WHERE srf.campaign_id = toUUID('32265e50-5a64-4acb-8ad1-5279d5c5cc2a') 
AND srf.created_at BETWEEN '2023-01-01' AND '2024-03-31'
AND srf.segment_name = 'brand'
AND rf.response_id IN (SELECT response_id FROM filtered_responses)
GROUP BY srf.segment_value
ORDER BY nps_score DESC;

-- 3. Trend Analysis by Month
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
WHERE srf.campaign_id = toUUID('32265e50-5a64-4acb-8ad1-5279d5c5cc2a') 
AND srf.created_at BETWEEN '2023-01-01' AND '2024-03-31'
AND srf.segment_name = 'brand'
GROUP BY 
    month,
    srf.segment_value
ORDER BY 
    month,
    nps_score DESC;

-- 4. Response Rate Analysis by Segment
WITH response_counts AS (
    SELECT 
        srf.segment_value as country,
        count(DISTINCT rf.response_id) as responses
    FROM segment_response_facts srf
    JOIN response_facts rf ON srf.response_id = rf.response_id
    WHERE srf.campaign_id = toUUID('32265e50-5a64-4acb-8ad1-5279d5c5cc2a') 
    AND srf.created_at BETWEEN '2023-01-01' AND '2024-03-31'
    AND srf.segment_name = 'country'
    GROUP BY srf.segment_value
),
delivery_counts AS (
    SELECT count(DISTINCT CASE WHEN is_delivered = 1 THEN participant_list_member_id END) as total_delivered
    FROM distribution_facts
    WHERE campaign_id = toUUID('32265e50-5a64-4acb-8ad1-5279d5c5cc2a') 
)
SELECT 
    rc.country,
    rc.responses,
    dc.total_delivered as invites_delivered,
    round(rc.responses * 100.0 / nullIf(dc.total_delivered, 0), 1) as response_rate
FROM response_counts rc
CROSS JOIN delivery_counts dc
ORDER BY response_rate DESC;

----------------------------------------------------------------------------------------
-- 1. Basic Segment Analysis with NPS Scores and Response Rate
SELECT 
    sm.segment_value as brand,
    sum(sm.response_count) as total_responses,
    sum(sm.promoters) as promoters,
    sum(sm.passives) as passives,
    sum(sm.detractors) as detractors,
    round((sum(sm.promoters) - sum(sm.detractors)) * 100.0 / nullIf(sum(sm.total_nps_responses), 0), 1) as nps_score,
    round(sum(sm.response_count) * 100.0 / nullIf(sum(rr.total_delivered), 0), 1) as response_rate
FROM segment_metrics_mv sm
LEFT JOIN response_rates_mv rr 
    ON sm.campaign_id = rr.campaign_id 
    AND sm.date = rr.date
WHERE sm.campaign_id = '{{your_campaign_id}}'  -- Replace with actual campaign_id
AND sm.date BETWEEN '2023-01-01' AND '2024-03-31'
AND sm.segment_name = 'brand'
GROUP BY sm.segment_value
ORDER BY nps_score DESC;

-- 2. Multi-segment Filter Analysis
WITH filtered_responses AS (
    SELECT DISTINCT srf.response_id
    FROM segment_response_facts srf
    WHERE srf.campaign_id = '{{your_campaign_id}}'
    AND srf.created_at BETWEEN '2023-01-01' AND '2024-03-31'
    AND (
        (srf.segment_name = 'country' AND srf.segment_value IN ('india', 'us'))
        OR (srf.segment_name = 'class' AND srf.segment_value = 'business')
    )
)
SELECT 
    sm.segment_value as brand,
    sum(sm.response_count) as total_responses,
    sum(sm.promoters) as promoters,
    sum(sm.passives) as passives,
    sum(sm.detractors) as detractors,
    round((sum(sm.promoters) - sum(sm.detractors)) * 100.0 / nullIf(sum(sm.total_nps_responses), 0), 1) as nps_score
FROM segment_metrics_mv sm
JOIN segment_response_facts srf ON srf.campaign_id = sm.campaign_id 
    AND srf.segment_name = sm.segment_name 
    AND srf.segment_value = sm.segment_value
WHERE sm.campaign_id = '{{your_campaign_id}}'
AND sm.date BETWEEN '2023-01-01' AND '2024-03-31'
AND sm.segment_name = 'brand'
AND srf.response_id IN (SELECT response_id FROM filtered_responses)
GROUP BY sm.segment_value
ORDER BY nps_score DESC;

-- 3. Trend Analysis by Month
SELECT 
    toStartOfMonth(sm.date) as month,
    sm.segment_value,
    sum(sm.response_count) as total_responses,
    sum(sm.promoters) as promoters,
    sum(sm.passives) as passives,
    sum(sm.detractors) as detractors,
    round((sum(sm.promoters) - sum(sm.detractors)) * 100.0 / nullIf(sum(sm.total_nps_responses), 0), 1) as nps_score
FROM segment_metrics_mv sm
WHERE sm.campaign_id = '{{your_campaign_id}}'
AND sm.date BETWEEN '2023-01-01' AND '2024-03-31'
AND sm.segment_name = 'brand'
GROUP BY 
    month,
    sm.segment_value
ORDER