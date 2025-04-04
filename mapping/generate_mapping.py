import os
import json
import time
import pandas as pd
import requests
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables from .env file
load_dotenv()

# Retrieve API keys from environment variables
API_NINJAS_API_KEY = os.getenv("API_NINJAS_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not API_NINJAS_API_KEY:
    raise ValueError("API_NINJAS_API_KEY is not configured in the .env file")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY is not configured in the .env file")

client = OpenAI(api_key=OPENAI_API_KEY)

def get_supported_activities():
    """
    Retrieve the list of supported activities from the API Ninjas endpoint.
    """
    url = "https://api.api-ninjas.com/v1/caloriesburnedactivities"
    headers = {"X-Api-Key": API_NINJAS_API_KEY}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        activities = response.json()
        if isinstance(activities, list):
            return activities
        else:
            raise ValueError("API response is not a list")
    else:
        raise Exception(f"Request error: {response.status_code} - {response.text}")

def map_exercise_to_activity_gpt4(exercise, activities):
    """
    Use GPT-4o to map a given exercise to the most similar supported activity.
    The prompt includes the list of supported activities and the exercise title,
    and instructs GPT-4o to return only the best matching activity name.
    """
    prompt = (
        f"Supported activities: {', '.join(activities)}.\n"
        f"For the exercise '{exercise}', which supported activity is the best match? "
        "Please provide only the activity name as your answer."
    )
    try:
        response = client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="gpt-4o"
        )
        answer = response.choices[0].message.content.strip()
        return answer
    except Exception as e:
        print(f"Error mapping exercise '{exercise}': {e}")

def generate_mapping(csv_file, output_file):
    """
    Generate a mapping from each unique exercise title in the CSV to the most similar supported activity using GPT-4.
    """
    # 1. Retrieve the supported activities list
    print("Fetching supported activities...")
    activities = get_supported_activities()
    print(f"Retrieved {len(activities)} activities.")

    # 2. Read the CSV and extract unique exercises
    print("Loading unique exercises from CSV...")
    df = pd.read_csv(csv_file)
    unique_exercises = df["Title"].unique()
    print(f"Found {len(unique_exercises)} unique exercises.")

    # 3. Map each exercise using GPT-4o
    print("Mapping exercises using GPT-4o...")
    mapping = {}
    for exercise in unique_exercises:
        best_activity = map_exercise_to_activity_gpt4(exercise, activities)
        mapping[exercise] = best_activity
        print(f"'{exercise}' mapped to '{best_activity}'")
        time.sleep(1)  # Sleep to avoid rate limits

    # 4. Save the mapping to a JSON file
    with open(output_file, "w") as f:
        json.dump(mapping, f, indent=4)
    print(f"Mapping saved to {output_file}")

if __name__ == "__main__":
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'megaGymDataset.csv')
    output_path = "mapping.json"
    generate_mapping(csv_path, output_path)
