import uuid
import random
from datetime import datetime, timedelta
import clickhouse_driver
import json
from faker import Faker
from typing import Dict, List, Tuple

# Initialize Faker
fake = Faker()

# ClickHouse connection
client = clickhouse_driver.Client(
    host='localhost',
    port=8000,
    user='default',
    password='',
    database='zs_db_1'
)

# Configuration
TOTAL_RESPONSES = 10000000  # 9M total responses
BATCH_SIZE = 20000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 3, 31)

# Campaign configurations with industry-specific segments and scoring patterns
CAMPAIGN_CONFIGS = {
    'airline_nps': {
        'industry': 'Airlines',
        'name': 'Airline Customer Experience Survey',
        'segments': {
            'brand': {
                'values': {
                    'indigo': 35,
                    'airasia': 20,
                    'spicejet': 15,
                    'airindia': 20,
                    'vistara': 10
                }
            },
            'class': {
                'values': {
                    'economy': 70,
                    'premium_economy': 15,
                    'business': 12,
                    'first': 3
                }
            },
            'route_type': {
                'values': {
                    'domestic': 65,
                    'international': 35
                }
            }
        },
        'nps_patterns': {
            'brand': {
                'indigo': {'promoter': 45, 'passive': 35, 'detractor': 20},
                'airasia': {'promoter': 35, 'passive': 40, 'detractor': 25},
                'spicejet': {'promoter': 30, 'passive': 35, 'detractor': 35},
                'airindia': {'promoter': 25, 'passive': 35, 'detractor': 40},
                'vistara': {'promoter': 40, 'passive': 35, 'detractor': 25}
            },
            'class': {
                'first': {'promoter': 60, 'passive': 30, 'detractor': 10},
                'business': {'promoter': 50, 'passive': 35, 'detractor': 15},
                'premium_economy': {'promoter': 40, 'passive': 40, 'detractor': 20},
                'economy': {'promoter': 30, 'passive': 40, 'detractor': 30}
            }
        }
    },
    'airline_lounge': {
        'industry': 'Airlines',
        'name': 'Airport Lounge Experience',
        'segments': {
            'lounge_type': {
                'values': {
                    'business': 60,
                    'first_class': 20,
                    'priority_pass': 20
                }
            },
            'visit_time': {
                'values': {
                    'morning': 30,
                    'afternoon': 35,
                    'evening': 35
                }
            },
            'services_used': {
                'values': {
                    'dining': 40,
                    'shower': 20,
                    'business_center': 20,
                    'spa': 20
                }
            }
        }
    },
    'airline_baggage': {
        'industry': 'Airlines',
        'name': 'Baggage Handling Feedback',
        'segments': {
            'baggage_type': {
                'values': {
                    'cabin': 45,
                    'checked': 55
                }
            },
            'journey_type': {
                'values': {
                    'domestic': 70,
                    'international': 30
                }
            },
            'handling_service': {
                'values': {
                    'standard': 70,
                    'priority': 30
                }
            }
        }
    }
}

def generate_realistic_nps_score(campaign_type: str, segment_values: Dict[str, str]) -> int:
    """Generate realistic NPS score based on segment patterns"""
    config = CAMPAIGN_CONFIGS[campaign_type]
    nps_patterns = config.get('nps_patterns', {})
    
    # Default probabilities if no specific pattern matches
    base_probabilities = {'promoter': 35, 'passive': 40, 'detractor': 25}
    
    # Adjust probabilities based on segments
    for segment_name, patterns in nps_patterns.items():
        if segment_name in segment_values:
            segment_value = segment_values[segment_name]
            if segment_value in patterns:
                base_probabilities = patterns[segment_value]
                break
    
    # Generate score based on adjusted probabilities
    category = random.choices(
        ['promoter', 'passive', 'detractor'],
        weights=[
            base_probabilities['promoter'],
            base_probabilities['passive'],
            base_probabilities['detractor']
        ]
    )[0]
    
    # Return score within appropriate range
    if category == 'promoter':
        return random.randint(9, 10)
    elif category == 'passive':
        return random.randint(7, 8)
    else:
        return random.randint(0, 6)

def generate_campaign_data():
    """Generate base data for campaigns and related configurations"""
    campaigns = []
    metric_configs = []
    question_segments = []
    segment_mapping = {}  # To store question_id mapping for each campaign-segment
    
    # Select 5 campaign types (can have duplicates)
    selected_campaigns = random.choices(list(CAMPAIGN_CONFIGS.keys()), k=5)
    
    for i, campaign_type in enumerate(selected_campaigns):
        campaign_config = CAMPAIGN_CONFIGS[campaign_type]
        campaign_id = str(uuid.uuid4())
        created_at = START_DATE + timedelta(days=random.randint(0, 30))
        
        # Store question_id mapping for this campaign
        segment_mapping[campaign_id] = {}
        
        # Create campaign
        campaigns.append({
            'campaign_id': campaign_id,
            'organization_id': str(uuid.uuid4()),
            'name': f"{campaign_config['name']} {i+1}",
            'industry': campaign_config['industry'],
            'created_at': created_at,
            'updated_at': created_at
        })
        
        # Create metric config
        metric_configs.append({
            'id': str(uuid.uuid4()),
            'campaign_id': campaign_id,
            'range_start': 0.00,
            'range_end': 10.00,
            'detractor_upper_range': 6.00,
            'promoter_lower_range': 9.00,
            'type': 'NPS',
            'promoter_label': 'Promoter',
            'detractor_label': 'Detractor',
            'passive_label': 'Passive',
            'metric_label': 'Net Promoter Score',
            'metric_label_short': 'NPS',
            'created_at': created_at,
            'updated_at': created_at
        })
        
        # Create segment mappings for this campaign
        for segment_name in campaign_config['segments'].keys():
            question_id = str(uuid.uuid4())
            segment_mapping[campaign_id][segment_name] = question_id
            
            question_segments.append({
                'question_id': question_id,
                'campaign_id': campaign_id,
                'segment_name': segment_name,
                'created_at': created_at,
                'updated_at': created_at
            })
    
    return campaigns, metric_configs, question_segments, selected_campaigns, segment_mapping

def generate_response_batch(campaign_id: str, campaign_type: str, segment_mapping: Dict, batch_size: int):
    """Generate batch of responses with proper referential integrity"""
    responses = []
    segment_responses = []
    distributions = []
    
    campaign_config = CAMPAIGN_CONFIGS[campaign_type]
    
    for _ in range(batch_size):
        response_id = str(uuid.uuid4())
        participant_id = str(uuid.uuid4())
        participant_list_member_id = str(uuid.uuid4())
        created_at = fake.date_time_between(START_DATE, END_DATE)
        
        # Generate segment values for this response
        response_segments = {}
        for segment_name, segment_data in campaign_config['segments'].items():
            values = list(segment_data['values'].keys())
            weights = list(segment_data['values'].values())
            response_segments[segment_name] = random.choices(values, weights=weights, k=1)[0]
        
        # Generate NPS score based on segments
        nps_score = generate_realistic_nps_score(campaign_type, response_segments)
        
        # Generate response record
        responses.append({
            'response_id': response_id,
            'campaign_id': campaign_id,
            'participant_id': participant_id,
            'participant_list_member_id': participant_list_member_id,
            'answers': json.dumps({'nps_score': nps_score}),
            'participant_info': json.dumps({
                'age_group': random.choice(['18-24', '25-34', '35-44', '45-54', '55+']),
                'gender': random.choice(['Male', 'Female', 'Other'])
            }),
            'created_at': created_at,
            'updated_at': created_at,
            'submission_type': 1,  # SUBMIT
            'participant_info_sensitive': '',
            'assisted_by': '',
            'calculated_answers_data': '{}',
            'calculated_response_data': '{}',
            'discarded': 0,
            'nps_score': nps_score,
            'csat_score': None,
            'ces_score': None
        })
        
        # Generate segment responses using proper question_ids
        for segment_name, segment_value in response_segments.items():
            question_id = segment_mapping[campaign_id][segment_name]
            segment_responses.append({
                'response_id': response_id,
                'campaign_id': campaign_id,
                'created_at': created_at,
                'question_id': question_id,  # Using mapped question_id
                'segment_value': segment_value,
                'discarded': 0
            })
        
        # Generate distribution data
        distributions.append({
            'campaign_id': campaign_id,
            'participant_list_member_id': participant_list_member_id,
            'schedule_id': str(uuid.uuid4()),
            'created_at': created_at - timedelta(days=random.randint(1, 7)),
            'sent_status': 2,  # SENT
            'is_delivered': 1 if random.random() < 0.95 else 0,
            'is_visited': 1 if random.random() < 0.70 else 0,
            'response_id': response_id
        })
    
    return responses, segment_responses, distributions

def insert_data():
    """Main function to insert data with proper relationships"""
    try:
        # Generate and insert base data
        campaigns, metric_configs, question_segments, selected_campaign_types, segment_mapping = generate_campaign_data()
        
        print("Inserting base data...")
        client.execute('INSERT INTO campaigns VALUES', campaigns)
        client.execute('INSERT INTO metric_configs VALUES', metric_configs)
        client.execute('INSERT INTO question_segment_mapping VALUES', question_segments)
        print("Base data inserted successfully")
        
        # Calculate responses per campaign
        responses_per_campaign = TOTAL_RESPONSES // len(campaigns)
        
        # Generate and insert response data
        total_responses = 0
        total_segments = 0
        
        for i, campaign in enumerate(campaigns):
            campaign_id = campaign['campaign_id']
            campaign_type = selected_campaign_types[i]
            print(f"\nGenerating data for campaign {campaign['name']}")
            
            for batch_num in range(0, responses_per_campaign, BATCH_SIZE):
                try:
                    batch_size = min(BATCH_SIZE, responses_per_campaign - batch_num)
                    responses, segment_responses, distributions = generate_response_batch(
                        campaign_id, 
                        campaign_type,
                        segment_mapping,
                        batch_size
                    )
                    
                    client.execute('INSERT INTO response_facts VALUES', responses)
                    client.execute('INSERT INTO segment_response_facts VALUES', segment_responses)
                    client.execute('INSERT INTO distribution_facts VALUES', distributions)
                    
                    total_responses += len(responses)
                    total_segments += len(segment_responses)
                    
                    print(f"Batch {batch_num // BATCH_SIZE + 1} inserted. "
                          f"Total responses: {total_responses:,}, "
                          f"Total segments: {total_segments:,}")
                    
                except Exception as e:
                    print(f"Error inserting batch: {str(e)}")
                    continue
        
        print(f"\nData generation completed successfully!")
        print(f"Total responses: {total_responses:,}")
        print(f"Total segment records: {total_segments:,}")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    insert_data()