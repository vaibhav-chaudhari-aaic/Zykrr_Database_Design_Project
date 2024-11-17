WITH segment_counts AS (
    SELECT 
        rf.campaign_id,
        srf.segment_value,
        COUNT(DISTINCT rf.response_id) as total_responses,
        COUNT(DISTINCT CASE WHEN rf.nps_score >= mc.promoter_lower_range THEN rf.response_id END) as promoters,
        COUNT(DISTINCT CASE WHEN rf.nps_score > mc.detractor_upper_range AND rf.nps_score < mc.promoter_lower_range THEN rf.response_id END) as passives,
        COUNT(DISTINCT CASE WHEN rf.nps_score <= mc.detractor_upper_range THEN rf.response_id END) as detractors
    FROM response_facts rf
    JOIN segment_response_facts srf ON rf.response_id = srf.response_id
    JOIN metric_configs mc ON rf.campaign_id = mc.campaign_id AND mc.type = 'NPS'
    JOIN question_segment_mapping qsm ON srf.question_id = qsm.question_id
    WHERE rf.campaign_id = '2ffa6ab7-effc-455c-a762-878fc92d9a07'
    AND qsm.segment_name = 'brand'
    AND NOT rf.discarded
    GROUP BY rf.campaign_id, srf.segment_value
)
SELECT 
    segment_value,
    total_responses,
    promoters,
    passives,
    detractors,
    ROUND((promoters - detractors)::float / NULLIF(total_responses, 0) * 100, 1) as nps_score
FROM segment_counts
ORDER BY nps_score DESC;




WITH segment_counts AS (
    SELECT 
        rd.campaign_id,
        rd.segment_value,
        COUNT(DISTINCT rd.response_id) as total_responses,
        COUNT(DISTINCT CASE WHEN rd.nps_score >= mc.promoter_lower_range THEN rd.response_id END) as promoters,
        COUNT(DISTINCT CASE WHEN rd.nps_score > mc.detractor_upper_range AND rd.nps_score < mc.promoter_lower_range THEN rd.response_id END) as passives,
        COUNT(DISTINCT CASE WHEN rd.nps_score <= mc.detractor_upper_range THEN rd.response_id END) as detractors
    FROM responses_denormalized rd
    JOIN metric_configs mc ON rd.campaign_id = mc.campaign_id AND mc.type = 1  -- NPS = 1
    JOIN question_segment_mapping qsm ON rd.question_id = qsm.question_id
    WHERE rd.campaign_id = '771b50b8-9306-476f-aed3-f90efd1240ac'
    AND qsm.segment_name = 'brand'
    AND NOT rd.discarded
    GROUP BY rd.campaign_id, rd.segment_value
)
SELECT 
    segment_value,
    total_responses,
    promoters,
    passives,
    detractors,
    ROUND((promoters - detractors) / NULLIF(total_responses, 0) * 100, 1) as nps_score
FROM segment_counts
ORDER BY nps_score DESC;



WITH segment_counts AS (
    SELECT 
        ra.campaign_id,
        seg.2 as segment_value,  -- seg.2 refers to segment_value in the tuple
        COUNT(DISTINCT ra.response_id) as total_responses,
        COUNT(DISTINCT CASE WHEN ra.nps_score >= mc.promoter_lower_range THEN ra.response_id END) as promoters,
        COUNT(DISTINCT CASE WHEN ra.nps_score > mc.detractor_upper_range AND ra.nps_score < mc.promoter_lower_range THEN ra.response_id END) as passives,
        COUNT(DISTINCT CASE WHEN ra.nps_score <= mc.detractor_upper_range THEN ra.response_id END) as detractors
    FROM responses_array ra
    ARRAY JOIN segment_data as seg  -- Unnest the array of segment tuples
    JOIN metric_configs mc ON ra.campaign_id = mc.campaign_id AND mc.type = 1  -- NPS = 1
    JOIN question_segment_mapping qsm ON seg.1 = qsm.question_id  -- seg.1 refers to question_id in the tuple
    WHERE ra.campaign_id = '62ad9143-b808-4b76-a20b-e66f88b36d49'
    AND qsm.segment_name = 'brand'
    AND NOT ra.discarded
    GROUP BY ra.campaign_id, seg.2
)
SELECT 
    segment_value,
    total_responses,
    promoters,
    passives,
    detractors,
    ROUND((promoters - detractors) / NULLIF(total_responses, 0) * 100, 1) as nps_score
FROM segment_counts
ORDER BY nps_score DESC;
