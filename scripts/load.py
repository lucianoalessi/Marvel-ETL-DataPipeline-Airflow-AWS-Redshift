from utils_bashOperator import *
import psycopg2
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

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


