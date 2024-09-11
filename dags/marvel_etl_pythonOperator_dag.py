from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils_pythonOperator import *
import hashlib
import psycopg2
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# --------- Requirements for making a request to the Marvel API: --------- #

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



# Function to check the connection with the Marvel API
def test_api_connection():

    endpoint = "http://gateway.marvel.com/v1/public/characters"

    try:
        response = requests.get(endpoint, params=params)
        # Check if the request was successful
        if response.status_code == 200:
            print("Successful connection to the Marvel API.")
            return True
        else:
            print(f"Error connecting to the Marvel API: {response.status_code} - {response.reason}")
            return False
    except requests.RequestException as e:
        print(f"Connection error to the Marvel API: {e}")
        return False

# Function to extract data from the Marvel API
def extract():

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

# Function to transform data extracted from the API
def transform():
    # -------------- Transformations df characters -------------- #

    # Fetch the characters dataframe
    characters_df = pd.read_parquet("/opt/airflow/datalake/bronze/marvel_api/characters/characters.parquet")

    # Fetch the character_comics dataframe
    character_comics_df = pd.read_parquet("/opt/airflow/datalake/bronze/marvel_api/characters/character_comics.parquet")

    # Add the 'comic_ids' column to the characters DataFrame by merging the DataFrames based on the 'character_id' column
    characters_df = characters_df.merge(character_comics_df, left_on='id', right_on='character_id', how='left')

    # Filter the columns of interest
    filter_columns = ['id', 'name', 'description', 'thumbnail.path', 'thumbnail.extension', 'comics.available', 'comic_ids', 'series.available', 'stories.available', 'modified']
    characters_df = characters_df[filter_columns]

    # Replace empty strings with NaN in the 'description', 'comics.available', etc. columns
    characters_df['description'].replace('', np.nan, inplace=True)
    characters_df['comics.available'].replace('', np.nan, inplace=True)
    characters_df['series.available'].replace('', np.nan, inplace=True)
    characters_df['stories.available'].replace('', np.nan, inplace=True)

    # Null value handling
    imputation_mapping = {
        "comics.available": 0,
        "series.available": 0,
        "stories.available": 0,
        "description": 'Description not available'
    }
    characters_df = characters_df.fillna(imputation_mapping)

    # Column type conversions, change the data type of numeric columns to a smaller type
    conversion_mapping = {
        "comics.available": "int16",
        "series.available": "int16",
        "stories.available": "int16",
        "thumbnail.extension": "category"
    }
    characters_df = characters_df.astype(conversion_mapping)

    # Date type conversions
    # From the 'modified' column, select only the first 10 characters which is just the date
    characters_df['modified'] = characters_df['modified'].str.slice(0, 10)
    # Convert 'modified' column to datetime, ignoring errors
    characters_df['modified'] = pd.to_datetime(characters_df['modified'], errors='coerce')
    # Replace NaT values with a predetermined date, which should be greater than 1970 for .parquet compatibility
    characters_df['modified'] = characters_df['modified'].fillna(pd.Timestamp('1971-01-01'))

    # Rename columns
    characters_df.rename(columns={
        "id": "character_id",
        "name": "character_name",
        "thumbnail.path": "thumbnail_path",
        "thumbnail.extension": "thumbnail_extension",
        "comics.available": "total_comics",
        "comic_ids": "character_comic_appearances",
        "series.available": "total_series",	
        "stories.available": "total_stories"
    }, inplace=True)

    characters_df['character_comic_appearances'] = characters_df['character_comic_appearances'].astype(str)

    characters_df.info(memory_usage='deep')

    # Store data in parquet format in the silver layer
    if not characters_df.empty:
        save_to_parquet(characters_df, "/opt/airflow/datalake/silver/marvel_api/characters/characters.parquet")

    # -------------- Transformations comics table -------------- #
    comics_df = pd.read_parquet("/opt/airflow/datalake/bronze/marvel_api/comics/comics.parquet")
    comics_filter_columns = ['id', 'digitalId', 'title', 'issueNumber', 'variantDescription', 'description', 'isbn', 'upc', 'diamondCode', 'characters.items', 'stories.available', 'events.available', 'modified']
    comics_df = comics_df[comics_filter_columns]
    comics_df

    # Replace empty strings with NaN in the columns
    comics_df['variantDescription'].replace('', np.nan, inplace=True)
    comics_df['description'].replace('', np.nan, inplace=True)
    comics_df['isbn'].replace('', np.nan, inplace=True)
    comics_df['upc'].replace('', np.nan, inplace=True)
    comics_df['diamondCode'].replace('', np.nan, inplace=True)

    # Null value handling
    imputation_mapping = {
        "variantDescription": 'Description not available',
        "description": 'Description not available',
        "isbn": 'No',
        "upc": 'No',
        "diamondCode": 'No'
    }
    comics_df = comics_df.fillna(imputation_mapping)

    # Column type conversions, change the data type of numeric columns to a smaller type
    conversion_mapping = {
        "id": "int32",
        "digitalId": "int32",
        "title": "category",
        "description": "category",
        "isbn": "category",
        "upc": "category",
        "diamondCode": "category",
        "issueNumber": "int16",
        "stories.available": "int16",
        "events.available": "int16"
    }
    comics_df = comics_df.astype(conversion_mapping)

    comics_df['characters.items'] = comics_df['characters.items'].astype(str)

    # Date type conversions
    # Convert 'modified' column to datetime, handling invalid values
    comics_df['modified'] = comics_df['modified'].str.slice(0, 10)
    # Convert 'modified' column to datetime, ignoring errors
    comics_df['modified'] = pd.to_datetime(comics_df['modified'], errors='coerce')
    # Replace NaT values with a predetermined date, which should be greater than 1970 for .parquet compatibility
    comics_df['modified'] = comics_df['modified'].fillna(pd.Timestamp('1970-01-01'))

    # Rename columns
    comics_df.rename(columns={
        "id": "comic_id",
        "digitalId": "comic_digitalId",
        "variantDescription": "variant_description",
        "diamondCode": "diamond_code",
        "issueNumber": "issue_number",	
        "stories.available": "total_stories",
        "events.available": "total_events",
        "characters.items": "characters_items"
    }, inplace=True)

    # Store data in parquet format in the silver layer
    if not comics_df.empty:
        save_to_parquet(comics_df, "/opt/airflow/datalake/silver/marvel_api/comics/comics.parquet")


# Function to load data into AWS Redshift
def load():

    # Looking for the files
    characters_df = pd.read_parquet("/opt/airflow/datalake/silver/marvel_api/characters/characters.parquet")
    comics_df = pd.read_parquet("/opt/airflow/datalake/silver/marvel_api/comics/comics.parquet")

    # Access environment variables
    redshift_host = os.getenv('REDSHIFT_HOST')
    redshift_db = os.getenv('REDSHIFT_DB')
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')
    redshift_port = os.getenv('REDSHIFT_PORT')

    # Establish connection with Redshift using psycopg2
    conn = psycopg2.connect(
        host=redshift_host,
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password,
        port=redshift_port
    )

    if conn:
        print('successful connection')
    else:
        print("Error connecting to Redshift")

    # Create a cursor to execute queries
    cur = conn.cursor()

    # Define queries to create tables in Redshift
    queries = [
        """
        CREATE TABLE IF NOT EXISTS public.characters (
            character_id INT NOT NULL PRIMARY KEY,
            character_name VARCHAR(255),
            description VARCHAR(MAX),
            thumbnail_path VARCHAR(255),
            thumbnail_extension VARCHAR(50),
            total_comics SMALLINT,
            character_comic_appearances VARCHAR(MAX),
            total_series SMALLINT,
            total_stories SMALLINT,
            modified TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS public.comics (
            comic_id INT NOT NULL PRIMARY KEY,
            comic_digitalId INT NOT NULL,
            title VARCHAR(255),
            issue_number SMALLINT,
            variant_description VARCHAR(MAX),
            description VARCHAR(MAX),
            isbn VARCHAR(50),
            upc VARCHAR(50),
            diamond_code VARCHAR(50),
            characters_items VARCHAR(MAX),
            total_stories SMALLINT,
            total_events SMALLINT,
            modified TIMESTAMP
        );
        """
    ]

    # Execute the queries to create the tables in Redshift
    for query in queries:
        cur.execute(query)

    # Commit the operations
    conn.commit()

    # Function to insert data using psycopg2
    def insert_data(cursor, conn, df, table_name):
        # Convert the DataFrame into a list of tuples
        data_tuples = list(df.itertuples(index=False, name=None))
        
        # Generate the SQL for data insertion
        placeholders = ', '.join(['%s'] * len(df.columns))
        columns = ', '.join(df.columns)
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Execute the insertion into Redshift
        cursor.executemany(insert_query, data_tuples)
        conn.commit()

    # Save the DataFrames to the Redshift database
    insert_data(cur, conn, characters_df, 'characters')
    insert_data(cur, conn, comics_df, 'comics')

    # Close the cursor and the connection
    cur.close()
    conn.close()

    # Print a message indicating the data insertion was completed
    print("Data insertion into Redshift database completed.")



# ---------------- DAG ---------------- #


default_args = {
    'owner': 'Luciano Alessi',
    'start_date': datetime(2024, 8, 27),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}


with DAG(
    dag_id='marvel_etl_pipeline_pythonOperator',
    default_args=default_args,
    description='This DAG extracts data from the Marvel API, transforming and loading information about characters and comics. The pipeline performs a daily incremental extraction, saves the data in parquet format, and stores it in a data lake.',
    schedule_interval="@daily",  
    catchup=False 
) as dag:
    
    test_api_connection_task = PythonOperator(
        task_id='test_api_connection',
        python_callable=test_api_connection
    )

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform
    )

    load_task = PythonOperator(
        task_id='load_data_redshift',
        python_callable=load
    )


    test_api_connection_task >> extract_task >> transform_task >> load_task


