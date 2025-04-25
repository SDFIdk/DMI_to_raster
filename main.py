from api.api_downloader import ClimateDataFetcher
from task_mapper import TaskMapper
from rasterizer import Rasterizer

import sys

def main(date_from, date_to, climate_parameter, output_folder):

    task_mapper = TaskMapper(date_from, date_to, climate_parameter)
    task_map = task_mapper.build_task_map()

    for task in task_map:

        download_task = ClimateDataFetcher(*task)
        climate_data = download_task.fetch_all_data()

        rasterizer = Rasterizer(climate_data, output_folder)
        rasterizer.build_raster()

        sys.exit()

    
def datetime_range_to_output(self, timerange: str) -> str:
    """
    Takes a string like '2020-01-10T00:00:00Z/2020-01-10T23:59:00Z'
    and returns 2020_01_10T00_00_00Z'
    Used to convert time ranges to path friendly strings
    """
    before_slash = timerange.split('/')[0]
    return before_slash.replace('-', '_').replace(':', '_')


if __name__ == '__main__':        

    """
    This script downloads and rasterizes climate data from DMI's free data API
    """

    date_from = "01012020"
    date_to = "01022020"

    climate_parameters = "pot_evaporation_makkink"

    output_folder = "output/"

    main(date_from, date_to, climate_parameters, output_folder)

