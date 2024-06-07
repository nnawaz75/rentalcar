# rentalcar
rentalcar demo
The following script generates a JSON file with 1 million unique rental session pairs (start and end events) for car rentals. Each session consists of a "START" record and an "END" record, each containing a unique session ID, a timestamp, and comments Result file is rental_data.json

Script name: gen_json_source.py

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


The following script reads rental session data from a JSON file, processes the data to compute session details, and writes the processed data to a CSV file. The script ensures data validity, calculates rental durations, checks for late returns and car damage, and outputs the results. 

Script name: import_json.py

import json
import csv
from datetime import datetime, timedelta, timezone

def validate_and_parse_json(file_path):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            if not isinstance(data, list):
                raise ValueError("JSON content is not a list")
            for record in data:
                if not isinstance(record, dict):
                    raise ValueError("JSON content does not contain a list of dictionaries")
        return data
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error parsing JSON: {e}")
        raise
def process_sessions(data):
    sessions = {}
    
    for record in data:
        session_id = record.get('id')
        if session_id not in sessions:
            sessions[session_id] = {'start': None, 'end': None, 'start_comments': '', 'end_comments': ''}
        if record['type'] == 'START':
            sessions[session_id]['start'] = int(record['timestamp'])
            sessions[session_id]['start_comments'] = record['comments']
        elif record['type'] == 'END':
            sessions[session_id]['end'] = int(record['timestamp'])
            sessions[session_id]['end_comments'] = record['comments']
    summary_records = []
    for session_id, details in sessions.items():
        if details['start'] and details['end']:  # Ensure both start and end exist
            start_time = datetime.fromtimestamp(details['start'], timezone.utc)
            end_time = datetime.fromtimestamp(details['end'], timezone.utc)
            duration = end_time - start_time
            returned_late = duration > timedelta(hours=24)
            car_damaged = bool(details['end_comments'])
            
            summary_records.append({
                'session_id': session_id,
                'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),  # Format as DATETIME string
                'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),  # Format as DATETIME string
                'duration': duration.total_seconds() / 3600,  # Convert to hours
                'late_return': 'True' if returned_late else 'False',
                'car_damaged': 'True' if car_damaged else 'False'
            })

    return summary_records

def write_summary_to_csv(summary_records, file_path):
    with open(file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['session_id', 'start_time', 'end_time', 'duration', 'late_return', 'car_damaged'])
        for record in summary_records:
            writer.writerow([
                record['session_id'],
                record['start_time'],
                record['end_time'],
                record['duration'],
                record['late_return'],
                record['car_damaged']
            ])
    print(f"Summary records written to {file_path}")

def main():
    json_file_path = 'rental_data.json'
    csv_file_path = 'output.csv'
    
    try:
        data = validate_and_parse_json(json_file_path)
        summary_records = process_sessions(data)
        write_summary_to_csv(summary_records, csv_file_path)
    except Exception as e:
        print(f"An error occurred: {e}")
if __name__ == "__main__":
    
main()

Remove header from resulting CSV file

tail -n +2 output.csv > new_output.csv

Copy the CSV file with 1 million rows to a AWS S3 bucket

aws s3 cp new_output.csv s3://awans3bucket/new_output.csv



Create a SingleStoreDB Cloud (trial account) Database size S00 and create a Database called rentalcar.
Create a memory and disk based table. 

CREATE TABLE IF NOT EXISTS rental_summary (
    session_id VARCHAR(255) PRIMARY KEY,
    start_time DATETIME,
    end_time DATETIME,
    duration DOUBLE,
    late_return VARCHAR(10),
    car_damaged VARCHAR(10)
);
Create and start a pipeline in the database to ingest the 1 million rows in parallel within a few seconds.

CREATE or replace PIPELINE pipe1 AS
LOAD DATA S3 'awans3bucket'
CREDENTIALS '{"aws_access_key_id": "BLANK", 
              "aws_secret_access_key": "BLANK"}'
              SKIP DUPLICATE KEY ERRORS
INTO TABLE rental_summary
   FIELDS TERMINATED BY ','
   LINES TERMINATED BY '\r\n';

start pipeline pipe1;

Review a portion of the data within the table for validation.

select * from rental_summary;


session_id
start_time
end_time
duration
late_return
car_damaged
UL45L3
2023-01-13 06:00:00
2023-01-14 12:00:00
30
TRUE
TRUE
8EWZFL
2023-04-18 05:00:00
2023-04-20 05:00:00
48
TRUE
TRUE
0WRQFH
2023-07-14 05:00:00
2023-07-14 13:00:00
8
FALSE
TRUE
2355N4
2023-09-23 05:00:00
2023-09-24 07:00:00
26
TRUE
TRUE
YFP75D
2023-09-19 05:00:00
2023-09-19 12:00:00
7
FALSE
TRUE
MVJ7YN
2023-08-23 05:00:00
2023-08-24 05:00:00
24
FALSE
TRUE
5P4X1C
2023-12-11 06:00:00
2023-12-13 00:00:00
42
TRUE
FALSE
V1WS6E
2023-04-27 05:00:00
2023-04-28 06:00:00
25
TRUE
TRUE
FHF977
2023-03-29 05:00:00
2023-03-30 04:00:00
23
FALSE
TRUE
6VSHF1
2023-05-03 05:00:00
2023-05-04 06:00:00
25
TRUE
TRUE
YF7C4A
2023-09-21 05:00:00
2023-09-23 05:00:00
48
TRUE
TRUE
XG1GTC
2023-03-02 06:00:00
2023-03-03 00:00:00
18
FALSE
TRUE
VH7Y3S
2023-07-06 05:00:00
2023-07-06 18:00:00
13
FALSE
TRUE
KCTMHM
2023-03-13 05:00:00
2023-03-15 00:00:00
43
TRUE
TRUE
Q9J09A
2023-04-06 05:00:00
2023-04-06 14:00:00
9
FALSE
TRUE
H7M49R
2023-09-18 05:00:00
2023-09-18 14:00:00
9
FALSE
TRUE
EKL5CS
2023-06-17 05:00:00
2023-06-19 03:00:00
46
TRUE
TRUE
URJ4XV
2023-07-15 05:00:00
2023-07-16 16:00:00
35
TRUE
TRUE
3YJS52
2023-07-08 05:00:00
2023-07-09 08:00:00
27
TRUE
TRUE


