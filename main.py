from api.api_downloader import ClimateDataFetcher
from task_mapper import TaskMapper
from rasterizer import Rasterizer

from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import sys
import threading


def process_task(task, output_folder, backoff_event):
    query_time, climate_parameter, time_interval = task

    download_task = ClimateDataFetcher(query_time, climate_parameter, backoff_event=backoff_event)
    climate_data = download_task.fetch_all_data()

    rasterizer = Rasterizer(climate_data, output_folder, time_interval)
    rasterizer.build_raster()


def main(date_from, date_to, climate_parameters, output_folder, temp_res="daily", num_threads=4):
    task_mapper = TaskMapper(date_from, date_to, climate_parameters, temp_res)
    task_map = task_mapper.build_task_map()

    print(f"Starting raster acquisition with {num_threads} threads...")

    backoff_event = threading.Event()

    futures = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for task in task_map:
            future = executor.submit(process_task, task, output_folder, backoff_event)
            futures.append(future)

        for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing tasks"):
            try:
                _.result()
            except Exception as e:
                tqdm.write(f"Error: {e}")

if __name__ == '__main__':        

    """
    Script for Downloading and Rasterizing Climate Data from DMI's Free Data API

    This script retrieves climate data from the Danish Meteorological Institute (DMI) 
    using their open data API and saves it as rasterized output.

    Parameters:
    -----------
    - date_from : str
        Start date of the data query, formatted as "DDMMYYYY".
    - date_to : str
        End date of the data query, formatted as "DDMMYYYY".
    - climate_parameters : list[str] or str
        Climate parameters to fetch, available at 10x10 km resolution. 
        See full list at:
        https://opendatadocs.dmi.govcloud.dk/Data/Climate_Data/Parameters_for_10x10_20x20_Municipality_and_Country_Values
    - temporal_resolution : str
        Temporal resolution of the data. Options:
            - "hourly": Attempts to fetch hourly data. Falls back to daily averages if hourly is unavailable.
            - "daily": Fetches daily averages directly.
    - output_folder : str
        Path to the directory where the fetched data will be saved.

    Authentication:
    ---------------
    An account at DMI is required to access the climate data API.
    Authentication is handled via a `.netrc` file.
    For more details, see:
    - DMI Authentication Guide: https://opendatadocs.dmi.govcloud.dk/en/Authentication
    - Netrc format documentation: 
        https://www.gnu.org/software/inetutils/manual/html_node/The-_002enetrc-file.html
        https://docs.python.org/3/library/netrc.html
    """

    date_from = "01012020"
    date_to = "03012020"

    climate_parameters = ["pot_evaporation_makkink", "mean_pressure"]
    # climate_parameters = ["pot_evaporation_makkink"]

    output_folder = "output/"

    temporal_resolution = "hourly"

    main(date_from, date_to, climate_parameters, output_folder, temp_res = temporal_resolution, num_threads=16)
