import uuid
import random
from datetime import datetime, timedelta
import clickhouse_driver
import json
from faker import Faker

# Initialize Faker for realistic data
fake = Faker()

# ClickHouse Docker connection
client = clickhouse_driver.Client(
    host='localhost',
    port=8000,
    user='default',
    password='',
    database='zykrr_analytics_2'
)

# Configuration
CAMPAIGNS = 10
RESPONSES_PER_CAMPAIGN = 1000000  # 1M responses per campaign
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 3, 31)

# More realistic sample data
SEGMENTS = {
    'country': {
        'india': 45,
        'us': 25,
        'uk': 15,
        'australia': 10,
        'canada': 5
    },
    'brand': {
        'indigo': 35,
        'airasia': 20,
        'spicejet': 15,
        'airindia': 20,
        'vistara': 10
    },
    'class': {
        'economy': 70,
        'premium_economy': 15,
        'business': 12,
        'first': 3
    },
    'travel_type': {
        'domestic': 65,
        'international': 35
    },
    'booking_channel': {
        'website': 40,
        'mobile_app': 35,
        'travel_agent': 15,
        'corporate': 10
    }
}

# NPS scoring patterns by segment
NPS_PATTERNS = {
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

def verify_tables():
    try:
        # Test queries to verify table structure
        client.execute('SELECT * FROM campaigns LIMIT 1')
        client.execute('SELECT * FROM response_facts LIMIT 1')
        client.execute('SELECT * FROM segment_response_facts LIMIT 1')
        client.execute('SELECT * FROM distribution_facts LIMIT 1')
        print("All tables verified successfully")
        return True
    except Exception as e:
        print(f"Table verification failed: {str(e)}")
        return False

def generate_campaign_data():
    campaigns = []
    campaign_types = ['Customer Satisfaction', 'Post Flight', 'Service Quality', 'Brand Experience']
    
    for i in range(CAMPAIGNS):
        created_at = START_DATE + timedelta(days=random.randint(0, 30))
        campaigns.append({
            'campaign_id': str(uuid.uuid4()),
            'organization_id': str(uuid.uuid4()),
            'name': f'{random.choice(campaign_types)} Survey {2023+i}',
            'industry': 'Airlines',
            'created_at': created_at,
            'updated_at': created_at
        })
    return campaigns

def generate_realistic_nps_score(segment_values):
    brand = segment_values['brand']
    travel_class = segment_values['class']
    
    brand_pattern = NPS_PATTERNS['brand'][brand]
    class_pattern = NPS_PATTERNS['class'][travel_class]
    
    combined_promoter = (brand_pattern['promoter'] * 0.6 + class_pattern['promoter'] * 0.4) / 100
    combined_passive = (brand_pattern['passive'] * 0.6 + class_pattern['passive'] * 0.4) / 100
    
    rand = random.random()
    if rand < combined_promoter:
        return random.randint(9, 10)
    elif rand < (combined_promoter + combined_passive):
        return random.randint(7, 8)
    else:
        return random.randint(0, 6)

def generate_response_data(campaign_id, response_count):
    responses = []
    segment_responses = []
    distributions = []
    
    batch_size = 10000
    
    for batch_start in range(0, response_count, batch_size):
        batch_end = min(batch_start + batch_size, response_count)
        
        for _ in range(batch_start, batch_end):
            created_at = START_DATE + timedelta(
                days=random.randint(0, (END_DATE - START_DATE).days)
            )
            while created_at.weekday() >= 5 and random.random() < 0.3:
                created_at += timedelta(days=1)
            
            response_segments = {}
            for segment_name, values in SEGMENTS.items():
                items, weights = zip(*values.items())
                response_segments[segment_name] = random.choices(items, weights=weights, k=1)[0]
            
            response_id = str(uuid.uuid4())
            participant_id = str(uuid.uuid4())
            participant_list_member_id = str(uuid.uuid4())
            
            nps_score = generate_realistic_nps_score(response_segments)
            
            # Generate response data
            response_data = {
                'answers': {
                    **response_segments,
                    'nps_score': nps_score,
                    'feedback': fake.text(max_nb_chars=200)
                },
                'participant_info': {
                    'age_group': random.choice(['18-25', '26-35', '36-45', '46-55', '55+']),
                    'frequency': random.choice(['First Time', 'Occasional', 'Regular', 'Frequent'])
                }
            }
            
            responses.append({
                'response_id': response_id,
                'campaign_id': campaign_id,
                'participant_id': participant_id,
                'participant_list_member_id': participant_list_member_id,
                'answers': json.dumps(response_data['answers']),
                'participant_info': json.dumps(response_data['participant_info']),
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
            
            # Generate segment responses
            for segment_name, value in response_segments.items():
                segment_responses.append({
                    'response_id': response_id,
                    'campaign_id': campaign_id,
                    'created_at': created_at,
                    'segment_name': segment_name,
                    'segment_value': value,
                    'question_id': str(uuid.uuid4()),
                    'discarded': 0
                })
            
            # Generate distribution data
            sent_date = created_at - timedelta(days=random.randint(1, 7))
            is_delivered = random.random() < 0.95  # 95% delivery rate
            is_visited = random.random() < 0.70 if is_delivered else 0  # 70% visit rate if delivered
            
            distributions.append({
                'campaign_id': campaign_id,
                'participant_list_member_id': participant_list_member_id,
                'schedule_id': str(uuid.uuid4()),
                'created_at': sent_date,
                'sent_status': 2,  # SENT
                'is_delivered': 1 if is_delivered else 0,
                'is_visited': 1 if is_visited else 0,
                'response_id': response_id
            })
        
        yield responses, segment_responses, distributions
        responses = []
        segment_responses = []
        distributions = []

def insert_data():
    # Insert campaigns
    campaigns = generate_campaign_data()
    client.execute('INSERT INTO campaigns VALUES', campaigns)
    print("Campaigns inserted")
    
    # Generate and insert data for each campaign
    for campaign in campaigns:
        print(f"Generating data for campaign {campaign['name']}")
        
        for batch_num, (responses, segment_responses, distributions) in enumerate(
            generate_response_data(campaign['campaign_id'], RESPONSES_PER_CAMPAIGN)
        ):
            try:
                client.execute('INSERT INTO response_facts VALUES', responses)
                client.execute('INSERT INTO segment_response_facts VALUES', segment_responses)
                client.execute('INSERT INTO distribution_facts VALUES', distributions)
                print(f"Inserted batch {batch_num + 1} for campaign {campaign['name']}")
            except Exception as e:
                print(f"Error inserting batch {batch_num + 1}: {str(e)}")
                continue

if __name__ == "__main__":
    try:
        if verify_tables():
            insert_data()
            print("Data generation completed successfully")
        else:
            print("Please verify table schema before proceeding")
    except Exception as e:
        print(f"Error occurred: {str(e)}")