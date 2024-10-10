import csv
import json
import os
import requests
import pandas as pd
import timeit
from concurrent.futures import ThreadPoolExecutor, as_completed

start_time = timeit.default_timer()

# URL pro stažení sportů a lig
url = 'https://www.tipsport.cz/rest/offer/v6/sports?fromResults=false'

# Hlavičky požadavku
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.tipsport.cz/kurzy/fotbal-16',
    'Origin': 'https://www.tipsport.cz',
    'Connection': 'keep-alive',
}

# Inicializace session a získání cookies
session = requests.Session()
initial_response = session.get('https://www.tipsport.cz', headers=headers)
cookies = session.cookies.get_dict()

# Funkce pro stahování dat pro jednotlivou ligu
def download_league_data(league_id):
    url_template = 'https://m.tipsport.cz/rest/offer/v1/competitions/{league_id}/matches?competitionId={league_id}&fromResults=false&matchViewGroupKey=WINNER_MATCH'
    url = url_template.format(league_id=league_id)
    response = session.get(url, headers=headers)

    if response.status_code == 200:
        file_path = os.path.join(output_folder, f"{league_id}.json")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(response.text)
        print(f"Data ID {league_id} saved to {file_path}")
    else:
        print(f"Error downloading league ID {league_id}. Status code: {response.status_code}")

# Paralelizace pro stahování lig
def parallel_league_download(df):
    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = {executor.submit(download_league_data, row['League ID']): row['League ID'] for _, row in df.iterrows()}
        for future in as_completed(futures):
            league_id = futures[future]
            try:
                future.result()  # Wait for the future to complete
            except Exception as e:
                print(f"League ID {league_id} generated an exception: {e}")

# Stažení sportů a lig
response = session.get(url, headers=headers, cookies=cookies)
if response.status_code == 200:
    data = response.json()
    with open('sports-leagues.json', 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)
    print("sports-leagues.json: OK")
else:
    print(f"Chyba: {response.status_code}")

# Zpracování stažených lig a zápis do CSV
with open('sports-leagues.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

output_csv = 'leagues.csv'
output_folder = 'odds'

if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Zápis lig do CSV
with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["League ID", "League URL", "League title"])

    def write_leagues(data):
        if isinstance(data, dict):
            if data.get('type') == 'COMPETITION':
                league_id = data.get('id', 'N/A')
                league_title = data.get('title', 'N/A')
                league_url = data.get('url', 'N/A')
                csvwriter.writerow([league_id, f"https://m.tipsport.cz{league_url}", league_title])
            for key in data:
                if isinstance(data[key], (dict, list)):
                    write_leagues(data[key])
        elif isinstance(data, list):
            for item in data:
                write_leagues(item)

    write_leagues(data)

# Paralelní stažení lig
df = pd.read_csv(output_csv)
parallel_league_download(df)

# Zpracování stažených zápasů
input_folder = 'odds'
output_folder = 'match-urls'
os.makedirs(output_folder, exist_ok=True)

def process_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    matches = []
    urls_set = set()

    for item in data.get('data', {}).get('children', []):
        if item['type'] == 'MATCH':
            match = item.get('match', {})
            match_id = match.get('id', 'N/A')
            match_name = match.get('name', 'N/A')
            match_url = f"https://m.tipsport.cz{match.get('url', 'N/A')}"

            for opp_row in match.get('oppRows', []):
                for opp in opp_row.get('oppsTab', []):
                    team_name = opp.get('label', 'N/A')
                    odds = opp.get('odd', 'N/A')
                    if match_url not in urls_set:
                        matches.append({
                            'Match ID': match_id,
                            'Match Name': match_name,
                            'Match URL': match_url
                        })
                        urls_set.add(match_url)

    return matches

# Paralelní zpracování JSON souborů
def process_json_files():
    matches_data = []
    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = {executor.submit(process_json_file, os.path.join(input_folder, filename)): filename for filename in os.listdir(input_folder) if filename.endswith('.json')}
        for future in as_completed(futures):
            filename = futures[future]
            try:
                matches = future.result()
                matches_data.extend(matches)
                print(f"Processed file: {filename}")
            except Exception as e:
                print(f"Error processing file {filename}: {e}")
    return matches_data

matches_data = process_json_files()
df = pd.DataFrame(matches_data)
df.to_csv(os.path.join(output_folder, "matches.csv"), index=False)
print(f"Data saved to: {os.path.join(output_folder, 'matches.csv')}")

# Stažení dat jednotlivých zápasů
input_folder = 'match-urls'
output_folder = 'odds-json'
os.makedirs(output_folder, exist_ok=True)

url_template = 'https://m.tipsport.cz/rest/offer/v3/matches/{match_id}?fromResults=false'

def fetch_match_data(match_id):
    url = url_template.format(match_id=match_id)
    print(f"Downloading data from url: {url}")
    try:
        response = session.get(url, headers=headers, cookies=cookies)
        response.raise_for_status()
        file_path = os.path.join(output_folder, f"{match_id}.json")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(response.text)
        print(f"Data ID {match_id} saved to: {file_path}")
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred for match ID {match_id}: {err}")
    except Exception as err:
        print(f"Other error occurred for match ID {match_id}: {err}")

# Paralelní stahování dat zápasů
def parallel_match_download(df):
    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = {executor.submit(fetch_match_data, row.get('Match ID')): row.get('Match ID') for _, row in df.iterrows()}
        for future in as_completed(futures):
            match_id = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"Match ID {match_id} generated an exception: {e}")

for filename in os.listdir(input_folder):
    if filename.endswith('.csv'):
        file_path = os.path.join(input_folder, filename)
        print(f"Processing file: {file_path}")
        if os.path.getsize(file_path) == 0:
            print(f"Soubor {file_path} je prázdný, bude přeskočen.")
            continue
        try:
            df = pd.read_csv(file_path)
            parallel_match_download(df)
        except pd.errors.EmptyDataError:
            print(f"Soubor {file_path} je prázdný, bude přeskočen.")
            continue
        except Exception as e:
            print(f"Chyba při čtení souboru {file_path}: {e}")
            continue

input_folder = 'odds-json'
output_folder = 'odds-csv'

# Ujistěte se, že složka pro výstup existuje
os.makedirs(output_folder, exist_ok=True)

def extract_match_data(data):
    """Extrahuje názvy týmů, kurzy a datum zápasu ze struktury JSON."""
    results = []

    match = data.get('match', {})
    match_name = match.get('nameFull', 'N/A')
    match_date = match.get('dateClosed', 'N/A')
    sport = match.get('superSport', 'N/A')

    for event_table in match.get('eventTables', []):
        table_name = event_table.get('name', 'N/A')

        for box in event_table.get('boxes', []):
            for cell in box.get('cells', []):
                team_name = cell.get('name', 'N/A')
                odd = cell.get('odd', 'N/A')
                results.append({
                    'Sport': sport,  # Přidání sportu do výstupu
                    'Match Name': match_name,
                    'Match Date': match_date,  # Přidání data zápasu do výstupu
                    'Event Table': table_name,
                    'Team Name': team_name,
                    'Odd': odd
                })

    return results



def process_json_file(file_path):
    """Zpracuje jeden JSON soubor a uloží výsledky do odpovídajícího CSV souboru."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    match_data = extract_match_data(data)

    # Vytvořte DataFrame a uložte do CSV souboru
    output_file = os.path.join(output_folder, f"{os.path.splitext(os.path.basename(file_path))[0]}.csv")
    df = pd.DataFrame(match_data)
    df.to_csv(output_file, index=False, encoding='utf-8')

    print(f"Data saved to: {output_file}")

def process_json_files_in_parallel():
    """Paralelizuje zpracování všech JSON souborů ve složce."""
    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = {executor.submit(process_json_file, os.path.join(input_folder, filename)): filename for filename in os.listdir(input_folder) if filename.endswith('.json')}
        for future in as_completed(futures):
            filename = futures[future]
            try:
                future.result()
                print(f"Processed file: {filename}")
            except Exception as e:
                print(f"Error processing file {filename}: {e}")

print("Processing JSON files in parallel...")
process_json_files_in_parallel()
print("All JSON files processed.")

end_time = timeit.default_timer()
execution_time = end_time - start_time
print(f"time: {execution_time:.2f} sekund")
