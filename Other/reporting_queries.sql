-- 1. Fetch Segment Values and Calculated NPS Percentage
WITH segment_stats AS (
    SELECT 
        segment_value,
        COUNT(*) AS total_responses,
        SUM(CASE WHEN promoter THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN passive THEN 1 ELSE 0 END) AS passives,
        SUM(CASE WHEN detractor THEN 1 ELSE 0 END) AS detractors,
        AVG(csat) AS average_csat
    FROM ResponseStat
    WHERE organization_id = '00010000-0000-0000-0000-000000000001'  -- Replace with your organization ID
      AND campaign_id = '10010000-0000-0000-0000-000000000001'  -- Replace with your campaign ID
      AND segment = 'Region'  -- Replace with the selected segment
    GROUP BY segment_value
)
SELECT 
    segment_value,
    total_responses,
    promoters,
    passives,
    detractors,
    (promoters - detractors) * 100.0 / NULLIF(total_responses, 0) AS nps_percentage
FROM segment_stats;


------------------------------------
-- Fetch Total Counts for Promoters, Passives, and Detractors
SELECT 
    SUM(CASE WHEN promoter THEN 1 ELSE 0 END) AS total_promoters,
    SUM(CASE WHEN passive THEN 1 ELSE 0 END) AS total_passives,
    SUM(CASE WHEN detractor THEN 1 ELSE 0 END) AS total_detractors
FROM ResponseStat
WHERE organization_id = '00010000-0000-0000-0000-000000000001'  -- Replace with your organization ID
AND campaign_id = '10010000-0000-0000-0000-000000000001';  -- Replace with your campaign ID

-- Fetch Total Counts for Promoters, Passives, and Detractors
SELECT 
    segment_value,
    COUNT(*) AS number_of_promoters
FROM ResponseStat
WHERE organization_id = '00010000-0000-0000-0000-000000000001'  -- Replace with your organization ID
  AND campaign_id = '10010000-0000-0000-0000-000000000001'  -- Replace with your campaign ID
  AND promoter = TRUE
GROUP BY segment_value
ORDER BY number_of_promoters DESC;

---
SELECT 
    segment_value,
    SUM(CASE WHEN detractor THEN 1 ELSE 0 END) AS detractors
FROM 
    ResponseStat
WHERE 
    organization_id = '00010000-0000-0000-0000-000000000001'
    AND campaign_id = '10010000-0000-0000-0000-000000000001'
    AND detractor = TRUE
GROUP BY 
    segment_value;

---
SELECT 
    segment_value,
    SUM(CASE WHEN passive THEN 1 ELSE 0 END) AS passives
FROM 
    ResponseStat
WHERE 
    organization_id = '00010000-0000-0000-0000-000000000001'
    AND campaign_id = '10010000-0000-0000-0000-000000000001'
    AND passive = TRUE
GROUP BY 
    segment_value;
---


-- Trend Analysis Over Time
SELECT 
    DATE_TRUNC('week', created_at) AS week,
    COUNT(*) AS total_responses,
    SUM(CASE WHEN promoter THEN 1 ELSE 0 END) AS promoters,
    SUM(CASE WHEN detractor THEN 1 ELSE 0 END) AS detractors,
    (SUM(CASE WHEN promoter THEN 1 ELSE 0 END) - SUM(CASE WHEN detractor THEN 1 ELSE 0 END)) * 100.0 / NULLIF(COUNT(*), 0) AS nps_percentage
FROM ResponseStat
WHERE organization_id = '00010000-0000-0000-0000-000000000001'  -- Replace with your organization ID
  AND campaign_id = '10010000-0000-0000-0000-000000000001'  -- Replace with your campaign ID
GROUP BY week
ORDER BY week;


-- Segment Comparison
SELECT 
    segment,
    COUNT(*) AS total_responses,
    SUM(CASE WHEN promoter THEN 1 ELSE 0 END) AS promoters,
    SUM(CASE WHEN passive THEN 1 ELSE 0 END) AS passives,
    SUM(CASE WHEN detractor THEN 1 ELSE 0 END) AS detractors,
    (SUM(CASE WHEN promoter THEN 1 ELSE 0 END) - SUM(CASE WHEN detractor THEN 1 ELSE 0 END)) * 100.0 / NULLIF(COUNT(*), 0) AS nps_percentage
FROM ResponseStat
WHERE organization_id = '00010000-0000-0000-0000-000000000001'  -- Replace with your organization ID
  AND campaign_id = '10010000-0000-0000-0000-000000000001'  -- Replace with your campaign ID
GROUP BY segment
ORDER BY nps_percentage DESC;



=========================================================================================
=========================================================================================





-- 1. Fetch Segment Values and Calculated NPS Percentage
WITH segment_stats AS (
    SELECT 
        segment_value,
        COUNT(*) AS total_responses,
        SUM(CASE WHEN promoter THEN 1 ELSE 0 END) AS promoters,
        SUM(CASE WHEN passive THEN 1 ELSE 0 END) AS passives,
        SUM(CASE WHEN detractor THEN 1 ELSE 0 END) AS detractors,
        AVG(csat) AS average_csat
    FROM ResponseStat
    WHERE organization_id = '00010000-0000-0000-0000-000000000001'
      AND campaign_id = '10010000-0000-0000-0000-000000000001'
      AND segment = 'Region'
    GROUP BY segment_value
)
SELECT 
    segment_value,
    total_responses,
    promoters,
    passives,
    detractors,
    (promoters - detractors) * 100.0 / NULLIF(total_responses, 0) AS nps_percentage
FROM segment_stats;

-- 2. Fetch Total Counts for Promoters, Passives, and Detractors
SELECT 
    SUM(CASE WHEN promoter THEN 1 ELSE 0 END) AS total_promoters,
    SUM(CASE WHEN passive THEN 1 ELSE 0 END) AS total_passives,
    SUM(CASE WHEN detractor THEN 1 ELSE 0 END) AS total_detractors
FROM ResponseStat
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001';

-- 3. Total Counts for Promoters by Segment
SELECT 
    segment_value,
    COUNT(*) AS number_of_promoters
FROM ResponseStat
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001'
  AND promoter = TRUE
GROUP BY segment_value
ORDER BY number_of_promoters DESC;

-- 4. Total Counts for Detractors by Segment
SELECT 
    segment_value,
    SUM(CASE WHEN detractor THEN 1 ELSE 0 END) AS detractors
FROM ResponseStat
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001'
  AND detractor = TRUE
GROUP BY segment_value;

-- 5. Total Counts for Passives by Segment
SELECT 
    segment_value,
    SUM(CASE WHEN passive THEN 1 ELSE 0 END) AS passives
FROM ResponseStat
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001'
  AND passive = TRUE
GROUP BY segment_value;

-- 6. Trend Analysis Over Time
SELECT 
    DATE_TRUNC('week', created_at) AS week,
    COUNT(*) AS total_responses,
    SUM(CASE WHEN promoter THEN 1 ELSE 0 END) AS promoters,
    SUM(CASE WHEN detractor THEN 1 ELSE 0 END) AS detractors,
    (SUM(CASE WHEN promoter THEN 1 ELSE 0 END) - SUM(CASE WHEN detractor THEN 1 ELSE 0 END)) * 100.0 / NULLIF(COUNT(*), 0) AS nps_percentage
FROM ResponseStat
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001'
GROUP BY week
ORDER BY week;

-- 7. Segment Comparison
SELECT 
    segment,
    COUNT(*) AS total_responses,
    SUM(CASE WHEN promoter THEN 1 ELSE 0 END) AS promoters,
    SUM(CASE WHEN passive THEN 1 ELSE 0 END) AS passives,
    SUM(CASE WHEN detractor THEN 1 ELSE 0 END) AS detractors,
    (SUM(CASE WHEN promoter THEN 1 ELSE 0 END) - SUM(CASE WHEN detractor THEN 1 ELSE 0 END)) * 100.0 / NULLIF(COUNT(*), 0) AS nps_percentage
FROM ResponseStat
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001'
GROUP BY segment
ORDER BY nps_percentage DESC;


===========================================================================

-- 1. Fetch Segment Values and Calculated NPS Percentage
WITH segment_stats AS (
    SELECT 
        segment_value,
        COUNT() AS total_responses,
        SUM(multiIf(promoter = 1, 1, 0)) AS promoters,
        SUM(multiIf(passive = 1, 1, 0)) AS passives,
        SUM(multiIf(detractor = 1, 1, 0)) AS detractors,
        AVG(csat) AS average_csat
    FROM reporting_analytics.responsestats
    WHERE organization_id = '00010000-0000-0000-0000-000000000001'
      AND campaign_id = '10010000-0000-0000-0000-000000000001'
      AND segment = 'Region'
    GROUP BY segment_value
)
SELECT 
    segment_value,
    total_responses,
    promoters,
    passives,
    detractors,
    (promoters - detractors) * 100.0 / NULLIF(total_responses, 0) AS nps_percentage
FROM segment_stats;

-- 2. Fetch Total Counts for Promoters, Passives, and Detractors
SELECT 
    SUM(multiIf(promoter = 1, 1, 0)) AS total_promoters,
    SUM(multiIf(passive = 1, 1, 0)) AS total_passives,
    SUM(multiIf(detractor = 1, 1, 0)) AS total_detractors
FROM reporting_analytics.responsestats
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001';

-- 3. Total Counts for Promoters by Segment
SELECT 
    segment_value,
    COUNT() AS number_of_promoters
FROM reporting_analytics.responsestats
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001'
  AND promoter = 1
GROUP BY segment_value
ORDER BY number_of_promoters DESC;

-- 4. Total Counts for Detractors by Segment
SELECT 
    segment_value,
    SUM(multiIf(detractor = 1, 1, 0)) AS detractors
FROM reporting_analytics.responsestats
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001'
  AND detractor = 1
GROUP BY segment_value;

-- 5. Total Counts for Passives by Segment
SELECT 
    segment_value,
    SUM(multiIf(passive = 1, 1, 0)) AS passives
FROM reporting_analytics.responsestats
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001'
  AND passive = 1
GROUP BY segment_value;

-- 6. Trend Analysis Over Time
SELECT 
    toStartOfWeek(created_at) AS week,
    COUNT() AS total_responses,
    SUM(multiIf(promoter = 1, 1, 0)) AS promoters,
    SUM(multiIf(detractor = 1, 1, 0)) AS detractors,
    (SUM(multiIf(promoter = 1, 1, 0)) - SUM(multiIf(detractor = 1, 1, 0))) * 100.0 / NULLIF(COUNT(), 0) AS nps_percentage
FROM reporting_analytics.responsestats
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001'
GROUP BY week
ORDER BY week;

-- 7. Segment Comparison
SELECT 
    segment,
    COUNT() AS total_responses,
    SUM(multiIf(promoter = 1, 1, 0)) AS promoters,
    SUM(multiIf(passive = 1, 1, 0)) AS passives,
    SUM(multiIf(detractor = 1, 1, 0)) AS detractors,
    (SUM(multiIf(promoter = 1, 1, 0)) - SUM(multiIf(detractor = 1, 1, 0))) * 100.0 / NULLIF(COUNT(), 0) AS nps_percentage
FROM reporting_analytics.responsestats
WHERE organization_id = '00010000-0000-0000-0000-000000000001'
  AND campaign_id = '10010000-0000-0000-0000-000000000001'
GROUP BY segment
ORDER BY nps_percentage DESC;
=============================================================


-- 1. Fetch Segment Values and Calculated NPS Percentage
--  -p-539ms -ch-118ms


-- 2. Fetch Total Counts for Promoters, Passives, and Detractors
--  -p-540ms -ch-63ms

-- 3. Total Counts for Promoters by Segment
--  -p-438ms -ch-   74ms

-- 4. Total Counts for Detractors by Segment
--  -p-290ms -ch-72ms


-- 5. Total Counts for Passives by Segment
--  -p-337ms -ch-70ms


-- 6. Trend Analysis Over Time
--  -p-1sec 345ms -ch-102ms


-- 7. Segment Comparison
--  -p-487ms -ch-89ms