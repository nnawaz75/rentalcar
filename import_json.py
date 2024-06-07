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
