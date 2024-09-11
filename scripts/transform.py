from utils_bashOperator import *

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
