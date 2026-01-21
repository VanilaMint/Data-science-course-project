import requests
import pandas as pd
import time
import os
import json
import csv
import concurrent.futures
import threading
from requests.exceptions import ConnectionError, Timeout, ChunkedEncodingError

API_KEY = "d32e92ea845b3549aed1fc1e5bda05b4"
INPUT_FILE = "id_list.csv"
OUTPUT_FILE = "tmdb_movie_details.csv"
SKIPPED_FILE = "skipped_movies.csv"

MAX_WORKERS_MOVIES = 8     

MAX_WORKERS_PEOPLE = 6     

BATCH_SIZE = 10            

DELAY_MAIN_LOOP = 0.1      

MAX_RETRIES = 5            
RETRY_BACKOFF = 2          

BASE_URL = "https://api.themoviedb.org/3"

skip_lock = threading.Lock()
stop_event = threading.Event() 

def safe_api_call(url, params):
    """
    Wrapper to handle network interruptions.
    Returns (json_data, None) if successful.
    Returns (None, error_message) if failed after retries.
    """
    for attempt in range(MAX_RETRIES):

        if stop_event.is_set():
            return None, "Script Stopped by User"

        try:
            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 429:
                time.sleep(RETRY_BACKOFF * (attempt + 1))
                continue

            response.raise_for_status()
            return response.json(), None

        except (ConnectionError, Timeout, ChunkedEncodingError) as e:
            wait_time = RETRY_BACKOFF * (attempt + 1)

            if not stop_event.is_set():
                print(f" Network Error. Retrying in {wait_time}s...")
                time.sleep(wait_time)

            if attempt == MAX_RETRIES - 1:
                return None, f"Max Retries (Network Error: {str(e)})"

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None, "404 Not Found"
            return None, f"HTTP Error {e.response.status_code}"
        except Exception as e:
            return None, f"Unexpected Error: {str(e)}"

    return None, "Unknown Error"

def log_skipped_id(imdb_id, reason):
    """Logs skipped IDs to a separate CSV file."""

    if stop_event.is_set():
        return

    with skip_lock:
        file_exists = os.path.exists(SKIPPED_FILE)
        try:
            with open(SKIPPED_FILE, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["imdb_id", "reason", "timestamp"])
                writer.writerow([imdb_id, reason, time.strftime("%Y-%m-%d %H:%M:%S")])
        except Exception as e:
            print(f"Error logging skip: {e}")

def get_person_credits(person_id):
    """
    Fetches combined credits for a person.
    Returns (list_of_credits, None) or (None, error_message).
    """
    if stop_event.is_set(): return None, "Stopped"

    url = f"{BASE_URL}/person/{person_id}/combined_credits"
    params = {"api_key": API_KEY}
    data, error = safe_api_call(url, params)

    if error:
        return None, error

    return (data.get("cast", []) + data.get("crew", [])), None

def get_prior_credit_count(credits_list, movie_release_date):
    """Counts credits strictly before the movie_release_date."""
    if not movie_release_date:
        return 0
    count = 0
    for credit in credits_list:
        c_date = credit.get("first_air_date") or credit.get("release_date")
        if not c_date:
            continue
        if c_date < movie_release_date:
            count += 1
    return count

def process_person_worker(person_info, current_movie_date):
    """
    Worker function to be run in parallel threads.
    """
    if stop_event.is_set():
        return None

    p_name = person_info['name']
    p_id = person_info['id']
    job_type = person_info['job_type']

    credits_list, error = get_person_credits(p_id)

    if error:

        if stop_event.is_set(): return None
        raise ValueError(f"Failed to fetch credits for {p_name}: {error}")

    count = get_prior_credit_count(credits_list, current_movie_date)

    return {
        "job_type": job_type,
        "data": {
            "name": p_name,
            "prior_count": count
        }
    }

def get_movie_details_by_imdb(imdb_id):
    """
    Fetches movie details. Handles specific error logging.
    """
    if stop_event.is_set(): return None

    find_url = f"{BASE_URL}/find/{imdb_id}"
    params = {"api_key": API_KEY, "external_source": "imdb_id"}
    data, error = safe_api_call(find_url, params)

    if error:
        log_skipped_id(imdb_id, f"Find API Failed: {error}")
        return None

    if not data or not data.get("movie_results"):
        log_skipped_id(imdb_id, "No TMDB Match Found")
        return None

    tmdb_id = data["movie_results"][0]["id"]

    details_url = f"{BASE_URL}/movie/{tmdb_id}"
    details_params = {"api_key": API_KEY, "append_to_response": "credits"}
    movie_data, error = safe_api_call(details_url, details_params)

    if error:
        log_skipped_id(imdb_id, f"Details API Failed: {error}")
        return None

    current_movie_date = movie_data.get("release_date")

    cast = movie_data.get("credits", {}).get("cast", [])[:5]
    crew = movie_data.get("credits", {}).get("crew", [])
    directors = [m for m in crew if m["job"] == "Director"]

    people_to_process = []
    for actor in cast:
        people_to_process.append({"name": actor["name"], "id": actor["id"], "job_type": "actor"})
    for director in directors:
        people_to_process.append({"name": director["name"], "id": director["id"], "job_type": "director"})

    actor_results = []
    director_results = []

    if people_to_process:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_PEOPLE) as executor:
            futures = {executor.submit(process_person_worker, person, current_movie_date): person for person in people_to_process}

            for future in concurrent.futures.as_completed(futures):
                if stop_event.is_set(): 
                    executor.shutdown(wait=False, cancel_futures=True)
                    return None

                try:
                    result = future.result()
                    if result:
                        formatted_data = result['data']
                        if result['job_type'] == 'actor':
                            actor_results.append({"actor": formatted_data["name"], "prior_count": formatted_data["prior_count"]})
                        else:
                            director_results.append({"director": formatted_data["name"], "prior_count": formatted_data["prior_count"]})
                except ValueError as ve:

                    if not stop_event.is_set():
                        print(f" Data incomplete for {imdb_id}: {ve}")
                        log_skipped_id(imdb_id, f"Partial Data Failure: {ve}")
                    return None 
                except Exception as exc:
                    if not stop_event.is_set():
                        print(f" Unexpected error for {imdb_id}: {exc}")
                        log_skipped_id(imdb_id, f"Unexpected Error: {exc}")
                    return None

    return process_movie_data(imdb_id, movie_data, actor_results, director_results)

def process_movie_data(imdb_id, data, actor_credits_data, director_credits_data):
    """Format final row."""
    credits = data.get("credits", {})
    crew = credits.get("crew", [])
    cast = credits.get("cast", [])

    directors = [m["name"] for m in crew if m["job"] == "Director"]
    actors = [m["name"] for m in cast[:5]]

    prod_companies = [c["name"] for c in data.get("production_companies", [])]
    prod_countries = [c["name"] for c in data.get("production_countries", [])]

    actors_json = json.dumps(actor_credits_data, separators=(',', ':'))
    directors_json = json.dumps(director_credits_data, separators=(',', ':'))

    return {
        "imdb_id": imdb_id,
        "tmdb_id": data.get("id"),
        "title": data.get("title"),
        "release_date": data.get("release_date"),
        "vote_average": data.get("vote_average"),
        "runtime": data.get("runtime"),
        "genres": ", ".join([g["name"] for g in data.get("genres", [])]),
        "director": ", ".join(directors),
        "top_cast": ", ".join(actors),
        "actors_other_works": actors_json,
        "directors_other_works": directors_json,
        "overview": data.get("overview"),
        "original_language": data.get("original_language"),
        "production_companies": ", ".join(prod_companies),
        "production_countries": ", ".join(prod_countries),
        "budget": data.get("budget"),
        "revenue": data.get("revenue"),
        "status": data.get("status")
    }

def get_processed_ids(output_file):
    """Reads processed IDs."""
    if not os.path.exists(output_file):
        return set()
    try:
        df = pd.read_csv(output_file, usecols=["imdb_id"])
        return set(df["imdb_id"].astype(str).str.strip())
    except:
        return set()

def save_batch(batch_data, output_file):
    """Saves a list of dictionaries to CSV."""
    if not batch_data:
        return

    file_exists = os.path.exists(output_file)
    df = pd.DataFrame(batch_data)

    df.to_csv(output_file, mode='a', header=not file_exists, index=False, quoting=csv.QUOTE_ALL)

def main():
    if API_KEY == "YOUR_TMDB_API_KEY":
        print("Error: Please replace 'YOUR_TMDB_API_KEY' inside the script.")
        return

    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    print("Reading input file...")
    try:
        df_input = pd.read_csv(INPUT_FILE, header=None, names=["imdb_id", "original_title"])
    except:
        df_input = pd.read_csv(INPUT_FILE, header=None, usecols=[0], names=["imdb_id"])

    all_ids = df_input["imdb_id"].astype(str).str.strip().tolist()
    processed_ids = get_processed_ids(OUTPUT_FILE)

    ids_to_process = [x for x in all_ids if x not in processed_ids]
    total = len(ids_to_process)

    if total == 0:
        print("All done!")
        return

    print(f"Starting fetch for {total} movies...")
    print(f"Batch Size: {BATCH_SIZE} | Movie Workers: {MAX_WORKERS_MOVIES} | Person Workers: {MAX_WORKERS_PEOPLE}")
    print("Press Ctrl+C to stop safely (Current batch will be saved).")

    batch_buffer = []

    try:

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_MOVIES) as executor:

            future_to_id = {executor.submit(get_movie_details_by_imdb, imdb_id): imdb_id for imdb_id in ids_to_process}

            for i, future in enumerate(concurrent.futures.as_completed(future_to_id)):

                if stop_event.is_set():
                    break

                imdb_id = future_to_id[future]
                print(f"[{i+1}/{total}] Processed {imdb_id}...", end="\r")

                try:
                    result = future.result()
                    if result:
                        batch_buffer.append(result)
                except Exception as exc:
                    if not stop_event.is_set():
                        print(f"\nGenerative Exception for {imdb_id}: {exc}")

                if len(batch_buffer) >= BATCH_SIZE or (i == total - 1):
                    save_batch(batch_buffer, OUTPUT_FILE)
                    batch_buffer = [] 

    except KeyboardInterrupt:
        print("\n\n!!! Stopping script (Ctrl+C Detected) !!!")
        print("Signaling threads to stop... please wait...")
        stop_event.set()

        if batch_buffer:
            print(f"Saving {len(batch_buffer)} pending items to disk...")
            save_batch(batch_buffer, OUTPUT_FILE)

        print("Exiting gracefully.")

    print(f"\n\nSuccess! Completed {total} items.")

if __name__ == "__main__":
    main()