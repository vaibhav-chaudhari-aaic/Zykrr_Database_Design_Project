-- Campaigns dimension
CREATE TABLE campaigns (
    campaign_id UUID,
    organization_id UUID,
    name String,
    industry LowCardinality(String),
    created_at DateTime,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY campaign_id;

-- Question-Segment mapping
CREATE TABLE question_segment_mapping (
    question_id UUID,
    campaign_id UUID,  -- Added campaign_id
    segment_name LowCardinality(String),
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (campaign_id, question_id); 

-- Metric configs
CREATE TABLE metric_configs (
    id UUID,
    campaign_id UUID,
    range_start Decimal(10,2),
    range_end Decimal(10,2),
    detractor_upper_range Decimal(10,2),
    promoter_lower_range Decimal(10,2),
    type Enum8('NPS' = 1, 'CSAT' = 2, 'CES' = 3),
    promoter_label String DEFAULT 'Promoter',
    detractor_label String DEFAULT 'Detractor',
    passive_label String DEFAULT 'Passive',
    metric_label String,
    metric_label_short String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (campaign_id, type);

-- Response facts
CREATE TABLE response_facts (
    response_id UUID,
    campaign_id UUID,
    participant_id UUID,
    participant_list_member_id UUID,
    answers String,  -- Changed from JSON to String
    participant_info String,  -- Changed from JSON to String
    created_at DateTime,
    updated_at DateTime,
    submission_type Enum8('SUBMIT' = 1, 'PARTIAL' = 2),
    participant_info_sensitive String,
    assisted_by String,
    calculated_answers_data String,  -- Changed from JSON to String
    calculated_response_data String,  -- Changed from JSON to String
    discarded UInt8,
    nps_score Nullable(Int8),
    csat_score Nullable(Int8),
    ces_score Nullable(Int8)
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (campaign_id, created_at, response_id);

-- Segment responses fact table
CREATE TABLE segment_response_facts (
    response_id UUID,
    campaign_id UUID,
    created_at DateTime,
    question_id UUID,
    segment_value String,
    discarded UInt8
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (campaign_id, question_id, segment_value, created_at);

-- Distribution tracking
CREATE TABLE distribution_facts (
    campaign_id UUID,
    participant_list_member_id UUID,
    schedule_id UUID,
    created_at DateTime,
    sent_status Enum8('PENDING' = 1, 'SENT' = 2, 'FAILED' = 3),
    is_delivered UInt8,
    is_visited UInt8,
    response_id Nullable(UUID)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (campaign_id, created_at);

-- Materialized Views
CREATE TABLE segment_metrics_mv (
    campaign_id UUID,
    question_id UUID,
    segment_value String,
    date Date,
    response_count UInt64,
    promoters UInt64,
    passives UInt64,
    detractors UInt64,
    total_nps_responses UInt64
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (campaign_id, question_id, segment_value, date);

CREATE MATERIALIZED VIEW segment_metrics_mv_view
TO segment_metrics_mv
AS 
SELECT 
    srf.campaign_id,
    srf.question_id,
    srf.segment_value,
    toDate(rf.created_at) as date,
    count(DISTINCT rf.response_id) as response_count,
    countIf(rf.nps_score >= mc.promoter_lower_range) as promoters,
    countIf(rf.nps_score > mc.detractor_upper_range AND rf.nps_score < mc.promoter_lower_range) as passives,
    countIf(rf.nps_score <= mc.detractor_upper_range) as detractors,
    count(DISTINCT CASE WHEN rf.nps_score IS NOT NULL THEN rf.response_id END) as total_nps_responses
FROM segment_response_facts srf
JOIN response_facts rf ON srf.response_id = rf.response_id
JOIN metric_configs mc ON rf.campaign_id = mc.campaign_id AND mc.type = 'NPS'
WHERE NOT rf.discarded
GROUP BY 
    srf.campaign_id,
    srf.question_id,
    srf.segment_value,
    toDate(rf.created_at);