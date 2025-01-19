import requests
from google.cloud import bigquery
import math
import os
from time import sleep

# Set GOOGLE_APPLICATION_CREDENTIALS to a file in the current directory
current_dir = os.path.dirname(os.path.abspath(__file__))
credentials_path = os.path.join(current_dir, "fatti-58546-97026e5de806.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Initialize BigQuery Client
client = bigquery.Client()
print("Gcloud client initialized!")

# Haversine Formula for Distance Calculation
def haversine(lat1, lon1, lat2, lon2):
    R = 6371e3  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (
        math.sin(dphi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# Query BigQuery for Coordinates
def fetch_coordinates():
    QUERY = """
    SELECT id, latitude, longitude, species
    FROM `fatti-58546.animalcrime.gbif`
    WHERE processed = FALSE
    LIMIT 1
    """

    QUERY = """
    SELECT gbifid, decimallatitude, decimallongitude, species
    FROM `fatti-58546.recklessroosters.gbif_src`
    LIMIT 1000
    """

    query_job = client.query(QUERY)
    return [
        (row.gbifid, row.decimallatitude, row.decimallongitude, row.species)
        for row in query_job.result()
    ]


# Query Overpass API for Nearest Roads
# See the OSM documentation for more details: https://wiki.openstreetmap.org/wiki/Key%3ahighway
def query_overpass(lat, lon):
    overpass_url = "http://overpass-api.de/api/interpreter"
    query = f"""
    [out:json];
    way(around:10,{lat},{lon})["highway"];
    out geom;
    """
    response = requests.post(overpass_url, data={"data": query})
    if response.status_code == 200:
        data = response.json()
        print(f"overpass response data: {data}")
        elements = data.get("elements", [])
        if not elements:
            return None
        # Find the nearest road using Haversine distance
        nearest_distance = float("inf")
        for element in elements:
            if "geometry" in element:
                for point in element["geometry"]:
                    dist = haversine(lat, lon, point["lat"], point["lon"])
                    nearest_distance = min(nearest_distance, dist)
        print(f"nearest_distance: {nearest_distance}")
        return nearest_distance
    else:
        print(f"Overpass API error: {response.status_code}")
        return None


# Process Data and Get Distances
def process_data():
    coordinates = fetch_coordinates()
    results = []
    for gbifid, lat, lon, species in coordinates:
        # sleep to hit overpass lightly. To still implement proper backoff algo here, but for now, this will do.
        sleep(1)
        distance = query_overpass(lat, lon)
        results.append((gbifid, lat, lon, species, distance))
    return results


# Sync Results Back to BigQuery
def sync_to_bigquery(results):
    table_id = "fatti-58546.animalcrime.gbif_distances"
    rows_to_insert = [
        {
            "gbifid": gbifid,
            "latitude": lat,
            "longitude": lon,
            "species": species,
            "distance": dist,
        }
        for gbifid, lat, lon, species, dist in results
    ]
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Failed to insert rows: {errors}")
    else:
        print("Data synced successfully!")


# Main Workflow
if __name__ == "__main__":
    print("Fetching coordinates...")
    results = process_data()
    if results:
        print(f"Found {len(results)} results, syncing to BigQuery...")
        sync_to_bigquery(results)
    else:
        print("No results to sync.")
