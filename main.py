# from api.api_downloader import ClimateDataFetcher
# from task_mapper import TaskMapper
# from rasterizer import Rasterizer

# from concurrent.futures import ThreadPoolExecutor, as_completed
# from tqdm import tqdm
# import sys

# def process_task(task, output_folder):
#     query_time, climate_parameter, time_interval = task

#     download_task = ClimateDataFetcher(query_time, climate_parameter)
#     climate_data = download_task.fetch_all_data()

#     rasterizer = Rasterizer(climate_data, output_folder, time_interval)
#     rasterizer.build_raster()

# def main(date_from, date_to, climate_parameters, output_folder, temp_res="daily", num_threads=4):
#     task_mapper = TaskMapper(date_from, date_to, climate_parameters, temp_res)
#     task_map = task_mapper.build_task_map()

#     print(f"Starting raster acquisition with {num_threads} threads...")

#     futures = []
#     with ThreadPoolExecutor(max_workers=num_threads) as executor:
#         for task in task_map:
#             future = executor.submit(process_task, task, output_folder)
#             futures.append(future)

#         for _ in tqdm(as_completed(futures), total=len(futures), desc="Processing tasks"):
#             try:
#                 _.result()  # Triggers error if one occurred
#             except Exception as e:
#                 tqdm.write(f"Error: {e}")



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
    This script downloads and rasterizes climate data from DMI's free data API
    """

    date_from = "01012020"
    date_to = "03012020"

    climate_parameters = ["pot_evaporation_makkink", "mean_pressure"]
    # climate_parameters = ["pot_evaporation_makkink"]

    output_folder = "output/"

    temporal_resolution = "hourly"

    main(date_from, date_to, climate_parameters, output_folder, temp_res = temporal_resolution, num_threads=16)
