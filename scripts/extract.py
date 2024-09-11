from utils_bashOperator import *
import hashlib
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# --------- Requirements for making a request to the Marvel API:   --------- #

# Obtain the keys from environment variables
public_key = os.getenv('MARVEL_PUBLIC_KEY')
private_key = os.getenv('MARVEL_PRIVATE_KEY')

if not public_key or not private_key:
    raise ValueError("API keys are not configured in environment variables")

# Generate the timestamp using datetime
ts = str(int(datetime.now().timestamp()))

# Generate the MD5 hash
hash_str = ts + private_key + public_key
md5_hash = hashlib.md5(hash_str.encode('utf-8')).hexdigest()  # hexdigest() converts the hash to a hexadecimal representation, which is more readable for humans and commonly used in applications that require hashes.

# Define params
params = {
    'ts': ts,
    'apikey': public_key,
    'hash': md5_hash
}

# --------- Define how often the extraction will be updated --------- #

# Path to store the last incremental extraction date
LAST_EXTRACTION_DATE_FILE = "/opt/airflow/datalake/bronze/marvel_api/last_extraction_date.txt"

# Get the date of the last incremental extraction
last_extraction_date = get_last_extraction_date(LAST_EXTRACTION_DATE_FILE)
current_date = datetime.now()

# Check if incremental extraction should run (once per day)
should_run_incremental = last_extraction_date is None or (current_date - last_extraction_date).days >= 1


# --------- Data Extraction --------- #

url_api = "http://gateway.marvel.com/v1/public"

# Data extraction of characters
endpoint_characters = "characters"

if should_run_incremental:
    data_characters = get_data(url_api, endpoint_characters, params)
    characters_df = build_table(data_characters)

    if not characters_df.empty:
        save_to_parquet(characters_df, "/opt/airflow/datalake/bronze/marvel_api/characters/characters.parquet")
    
    # Create a dataframe with a list of comics in which each character appears
    # List to store the character-comics relationship
    character_comics = []

    # Iterate over each character in the characters results list
    for character in data_characters:
        character_id = character['id']
        comics = character['comics']['items']
        comic_ids = []

        for comic in comics:
            parts = comic['resourceURI'].split('/')
            comic_id = parts[-1]
            comic_ids.append(comic_id)
        
        relationship = {
            'character_id': character_id,
            'comic_ids': comic_ids
        }
        
        character_comics.append(relationship)
    
    character_comics_df = build_table(character_comics)

    if not character_comics_df.empty:
        save_to_parquet(character_comics_df, "/opt/airflow/datalake/bronze/marvel_api/characters/character_comics.parquet")

# Data extraction of comics
endpoint_comics = "comics"

if should_run_incremental:
    data_comics = get_comics(url_api, endpoint_comics, params)
    comics_df = build_table(data_comics)

    if not comics_df.empty:
        save_to_parquet(comics_df, "/opt/airflow/datalake/bronze/marvel_api/comics/comics.parquet")

# Update the last incremental extraction date if new data was obtained
if should_run_incremental and not comics_df.empty:
    last_extraction_date = datetime.now()
    update_last_extraction_date(LAST_EXTRACTION_DATE_FILE, last_extraction_date)
    # Print a message indicating that the extraction and data saving were completed
    print("Extraction and data saving completed.")
else:
    print("No updates")