-- SQL Schema for Reporting and Analytics Subsystem


CREATE TABLE Filter (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE FilterOption (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    filter_id UUID NOT NULL REFERENCES Filter(id),
    name VARCHAR(255) NOT NULL
);

CREATE TABLE ResponseStat (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),  
    organization_id UUID NOT NULL,                  
    campaign_id UUID NOT NULL,                      
    segment VARCHAR(255),                          
    segment_value VARCHAR(255),                    
    promoter BOOLEAN,                              
    passive BOOLEAN,                               
    detractor BOOLEAN,                             
    csat DECIMAL(5, 2),                            
    created_by UUID,                               
    updated_by UUID,                               
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE FilterOptionResponseStat (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    filter_option_id UUID NOT NULL REFERENCES FilterOption(id),
    response_stat_id UUID NOT NULL REFERENCES ResponseStat(id)
);


CREATE TABLE Segment (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    name VARCHAR(255) NOT NULL
);


CREATE TABLE SegmentValue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    segment_id UUID NOT NULL REFERENCES Segment(id),
    name VARCHAR(255) NOT NULL
);


CREATE TABLE SegmentValueResponseStat (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL,
    segment_value_id UUID NOT NULL REFERENCES SegmentValue(id),
    response_stat_id UUID NOT NULL REFERENCES ResponseStat(id)
);


CREATE INDEX idx_response_stat_campaign ON ResponseStat (campaign_id);
CREATE INDEX idx_response_stat_segment ON ResponseStat (segment, segment_value);
CREATE INDEX idx_response_stat_promoter ON ResponseStat (promoter);
CREATE INDEX idx_response_stat_passive ON ResponseStat (passive);
CREATE INDEX idx_response_stat_detractor ON ResponseStat (detractor);

-- SELECT segment_value, COUNT(id) as count_promoters
-- FROM ResponseStat
-- WHERE promoter = true AND campaign_id = '<campaign_id>'
-- GROUP BY segment_value;


-- DO $$
-- DECLARE
--     org_id UUID := '00010000-0000-0000-0000-000000000001';  -- Example organization ID
--     campaign_id_1 UUID := '10010000-0000-0000-0000-000000000001';  -- Campaign ID 1
--     campaign_id_2 UUID := '10020000-0000-0000-0000-000000000002';  -- Campaign ID 2
--     segments TEXT[] := ARRAY['Region', 'Product', 'Service', 'Demographics', 'Market'];  -- Segment types
--     segment_values TEXT[] := ARRAY[
--         'North America', 'Europe', 'Asia', 
--         'Electronics', 'Clothing', 'Home Appliances', 
--         'Cosmetics', 'Books', 'Toys', 
--         'Teens', 'Adults', 'Seniors'
--     ];  -- Segment values
--     created_at TIMESTAMP;
-- BEGIN
--     FOR i IN 1..10000 LOOP
--         -- Generate a random timestamp for created_at
--         created_at := NOW() - (random() * interval '30 days');  -- Random date within the last 30 days

--         INSERT INTO ResponseStat (id, organization_id, campaign_id, segment, segment_value, promoter, passive, detractor, csat, created_at) 
--         VALUES (
--             gen_random_uuid(),  -- Generate a new UUID for the id
--             org_id,
--             CASE WHEN i % 2 = 0 THEN campaign_id_1 ELSE campaign_id_2 END,  -- Alternate between two campaigns
--             segments[(i % array_length(segments, 1)) + 1],  -- Random segment
--             segment_values[(i % array_length(segment_values, 1)) + 1],  -- Random segment value
--             (RANDOM() < 0.5),  -- Random true/false for promoter
--             (RANDOM() < 0.5),  -- Random true/false for passive
--             (RANDOM() < 0.5),  -- Random true/false for detractor
--             ROUND((RANDOM() * 5)::numeric, 2),  -- Random CSAT score between 0 and 5
--             created_at
--         );
--     END LOOP;
-- END $$;


-- DROP TABLE IF EXISTS FilterOptionResponseStat CASCADE;
-- DROP TABLE IF EXISTS SegmentValueResponseStat CASCADE;
-- DROP TABLE IF EXISTS FilterOption CASCADE;
-- DROP TABLE IF EXISTS SegmentValue CASCADE;
-- DROP TABLE IF EXISTS Segment CASCADE;
-- DROP TABLE IF EXISTS ResponseStat CASCADE;
-- DROP TABLE IF EXISTS Filter CASCADE;
