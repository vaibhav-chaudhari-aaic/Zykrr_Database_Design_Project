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
    database='zs_db_2'
)

# Configuration
TOTAL_RESPONSES = 10000000  # 9M total responses
BATCH_SIZE = 20000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 3, 31)

# Campaign configurations
CAMPAIGN_CONFIGS = {
    'airline_feedback': {
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
                },
                'multiple': True
            },
            'class': {
                'values': {
                    'economy': 70,
                    'premium_economy': 15,
                    'business': 12,
                    'first': 3
                },
                'multiple': True
            },
            'route_type': {
                'values': {
                    'domestic': 65,
                    'international': 35
                },
                'multiple': False
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
                },
                'multiple': False
            },
            'visit_time': {
                'values': {
                    'morning': 30,
                    'afternoon': 35,
                    'evening': 35
                },
                'multiple': True
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
                },
                'multiple': False
            },
            'handling_speed': {
                'values': {
                    'fast': 30,
                    'average': 50,
                    'slow': 20
                },
                'multiple': False
            }
        }
    }
}

def generate_campaign_data():
    """Generate base data with proper relationships"""
    campaigns = []
    metric_configs = []
    question_segments = []
    segment_mapping = {}  # Store question_ids for each campaign-segment
    
    # Select 5 campaigns (allowing duplicates with different names)
    selected_campaign_types = random.choices(list(CAMPAIGN_CONFIGS.keys()), k=5)
    
    for i, campaign_type in enumerate(selected_campaign_types):
        campaign_config = CAMPAIGN_CONFIGS[campaign_type]
        campaign_id = str(uuid.uuid4())
        created_at = START_DATE + timedelta(days=random.randint(0, 30))
        
        # Create campaign
        campaigns.append({
            'campaign_id': campaign_id,
            'organization_id': str(uuid.uuid4()),
            'name': f"{campaign_config['name']} {i+1}",  # Add number to make names unique
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
            'type': 1,  # NPS = 1
            'promoter_label': 'Promoter',
            'detractor_label': 'Detractor',
            'passive_label': 'Passive',
            'metric_label': 'Net Promoter Score',
            'metric_label_short': 'NPS',
            'created_at': created_at,
            'updated_at': created_at
        })
        
        # Create question-segment mappings and store for later use
        segment_mapping[campaign_id] = {}
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
    
    return campaigns, metric_configs, question_segments, selected_campaign_types, segment_mapping

def generate_response_batch(campaign_id: str, campaign_type: str, segment_mapping: Dict, batch_size: int):
    """Generate response batch with proper foreign key relationships"""
    denormalized_responses = []
    distributions = []
    campaign_config = CAMPAIGN_CONFIGS[campaign_type]
    
    for _ in range(batch_size):
        response_id = str(uuid.uuid4())
        participant_id = str(uuid.uuid4())
        participant_list_member_id = str(uuid.uuid4())
        created_at = fake.date_time_between(START_DATE, END_DATE)
        
        # Generate NPS score (0-10)
        nps_score = random.choices(
            population=range(0, 11),
            weights=[4, 3, 4, 5, 6, 8, 10, 12, 18, 15, 15],
            k=1
        )[0]
        
        # Base response data
        base_response = {
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
        }
        
        # Generate denormalized records for each segment
        for segment_name, segment_config in campaign_config['segments'].items():
            values = list(segment_config['values'].keys())
            weights = list(segment_config['values'].values())
            
            # If segment can have multiple values, generate 1-3 values
            if segment_config.get('multiple', False):
                num_values = random.randint(1, min(3, len(values)))
                selected_values = random.choices(
                    population=values,
                    weights=weights,
                    k=num_values
                )
            else:
                selected_values = [random.choices(
                    population=values,
                    weights=weights,
                    k=1
                )[0]]
            
            # Create a denormalized record for each value using proper question_id
            for segment_value in selected_values:
                denormalized_record = base_response.copy()
                denormalized_record['question_id'] = segment_mapping[campaign_id][segment_name]
                denormalized_record['segment_value'] = segment_value
                denormalized_responses.append(denormalized_record)
        
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
    
    return denormalized_responses, distributions

def insert_data():
    """Main function to insert data"""
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
        total_denormalized_records = 0
        
        for i, campaign in enumerate(campaigns):
            campaign_id = campaign['campaign_id']
            campaign_type = selected_campaign_types[i]
            print(f"\nGenerating data for campaign {campaign['name']}")
            
            for batch_num in range(0, responses_per_campaign, BATCH_SIZE):
                try:
                    batch_size = min(BATCH_SIZE, responses_per_campaign - batch_num)
                    denormalized_responses, distributions = generate_response_batch(
                        campaign_id, 
                        campaign_type,
                        segment_mapping,
                        batch_size
                    )
                    
                    client.execute('INSERT INTO responses_denormalized VALUES', denormalized_responses)
                    client.execute('INSERT INTO distribution_facts VALUES', distributions)
                    
                    total_responses += batch_size
                    total_denormalized_records += len(denormalized_responses)
                    
                    print(f"Batch {batch_num // BATCH_SIZE + 1} inserted. "
                          f"Total responses: {total_responses:,}, "
                          f"Total denormalized records: {total_denormalized_records:,}")
                    
                except Exception as e:
                    print(f"Error inserting batch: {str(e)}")
                    continue
        
        print(f"\nData generation completed successfully!")
        print(f"Total responses: {total_responses:,}")
        print(f"Total denormalized records: {total_denormalized_records:,}")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")

if __name__ == "__main__":
    insert_data()