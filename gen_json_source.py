import json
import random
from datetime import datetime, timedelta
# Function to generate random timestamps
def random_timestamp(start, end):
    return int(random.uniform(start.timestamp(), end.timestamp()))
# Function to generate a random session id
def random_session_id():
    return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=6))
# Function to generate random comments
def random_comments(damaged):
    if damaged:
        return random.choice([
            "Car is missing both front wheels!",
            "Front bumper is damaged.",
            "Scratch on driver's door.",
            "Crack found in front passenger window.",
            "Rear lights are broken."
        ])
    else:
        return random.choice([
            "No issues - brand new and shiny!",
            "No new damage observed.",
            "Small dent on passenger door",
            "Clean and in good condition.",
            ""
        ])
# Generate data
data = []
unique_ids = set()
while len(unique_ids) < 1000000:
    session_id = random_session_id()
    if session_id in unique_ids:
        continue
    unique_ids.add(session_id)
    # Randomly decide if the rental will be more than 24 hours and if it will have damage
    duration_more_than_24_hours = random.choice([True, False])
    car_damaged = random.choice([True, False])
    start_time = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
    end_time = start_time + timedelta(hours=random.randint(25, 48) if duration_more_than_24_hours else random.randint(1, 24))
    start_record = {
        "type": "START",
        "id": session_id,
        "timestamp": str(random_timestamp(start_time, start_time)),
        "comments": random_comments(False)
    }
    end_record = {
        "type": "END",
        "id": session_id,
        "timestamp": str(random_timestamp(end_time, end_time)),
        "comments": random_comments(car_damaged)
    }
    data.append(start_record)
    data.append(end_record)
# Save to json file
with open('rental_data.json', 'w') as json_file:
    json.dump(data, json_file, indent=4)

