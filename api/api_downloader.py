import requests
import netrc
import sys

# Retrieve API key from .netrc
NETRC_MACHINE = "dmigw.govcloud.dk"
API_KEY = netrc.netrc().authenticators(NETRC_MACHINE)[2]

BASE_URL = "https://dmigw.govcloud.dk/v2/climateData/collections/10kmGridValue/items"


class ClimateDataFetcher:
    def __init__(self, datetime_range, climate_parameter):
        self.datetime_range = datetime_range
        self.parameter_id = climate_parameter


    def fetch_all_data(self, limit = 10000):
        all_data = []
        unique_ids = set()
        next_url = BASE_URL  # Start with the base URL
        
        params = {
            "datetime": self.datetime_range,
            "parameterId": self.parameter_id,
            "bbox-crs": "https://www.opengis.net/def/crs/OGC/1.3/CRS84",
            "api-key": API_KEY,
            "limit": limit
        }
        
        while next_url:
            response = requests.get(next_url, params=params if next_url == BASE_URL else None)
            # print(f"Fetching: {response.url}")
            
            if response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                break
            
            data = response.json()
            if not isinstance(data, dict) or "features" not in data:
                print("Unexpected response format", data)
                break
            
            for entry in data["features"]:
                entry_id = entry.get("id")
                if entry_id and entry_id not in unique_ids:
                    unique_ids.add(entry_id)
                    all_data.append(entry)
            
            print(f"Total unique records fetched so far: {len(all_data)}")
            
            # Check for pagination link
            next_url = None
            for link in data.get("links", []):
                if link.get("rel") == "next":
                    next_url = link.get("href")
                    break
        
        return all_data
