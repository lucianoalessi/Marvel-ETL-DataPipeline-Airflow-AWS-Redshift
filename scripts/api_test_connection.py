from utils_bashOperator import *
import hashlib

def test_api_connection():

    endpoint = "http://gateway.marvel.com/v1/public/characters"

    # Obtain the keys from environment variables
    public_key = os.getenv('MARVEL_PUBLIC_KEY')
    private_key = os.getenv('MARVEL_PRIVATE_KEY')

    if not public_key or not private_key:
        raise ValueError("API keys are not set in the environment variables")

    # Generate the timestamp using datetime
    ts = str(int(datetime.now().timestamp()))

    # Generate the MD5 hash
    hash_str = ts + private_key + public_key 
    md5_hash = hashlib.md5(hash_str.encode('utf-8')).hexdigest()

    # Define params
    params = {
        'ts': ts,
        'apikey': public_key,
        'hash': md5_hash
    }

    try:
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            print("Successful connection to the Marvel API.")
            return True
        else:
            print(f"Error connecting to the Marvel API: {response.status_code} - {response.reason}")
            return False
    except requests.RequestException as e:
        print(f"Connection error to the Marvel API: {e}")
        return False

test_api_connection()
