from datetime import datetime, timedelta
from itertools import product
import sys

class TaskMapper:
    """
    Converts date and parameter query into a list of specific query tasks
    """
    def __init__(
            self, 
            date_from, 
            date_to, 
            climate_parameters, 
            temp_res,
            api_output="api_output/", 
        ):

        # self.datetime_range = self.format_datetime_range(date_from, date_to)

        if isinstance(climate_parameters, str):
            climate_parameters = [climate_parameters]
        elif not isinstance(climate_parameters, list):
            climate_parameters = list(climate_parameters)
        self.climate_parameters = climate_parameters

        self.api_output = api_output

        self.date_from = date_from
        self.date_to = date_to

        self.temporal_resolution = temp_res


    def build_task_map(self):

        if self.temporal_resolution == "daily":
            date_list = self.generate_date_list(self.date_from, self.date_to, "daily")
            # task_map = list(product(date_list, self.climate_parameters))

            task_map = [[a, b, "daily"] for a, b in product(date_list, self.climate_parameters)]


        elif self.temporal_resolution == "hourly":
            # piecewise map of daily and hourly

            day_list = self.generate_date_list(self.date_from, self.date_to, "daily")
            hour_list = self.generate_date_list(self.date_from, self.date_to, "hourly")

            hourly_params = []
            daily_params = []
            for param in self.climate_parameters:
                if "hourly" in self.get_parameter_temp_res()[param]:
                    hourly_params.append(param)
                else:
                    daily_params.append(param)

            if hourly_params:
                print(f"{hourly_params} will be acquired in hourly intervals")
            else:
                print("No selected parameters availeble in hourly intervals")
            if daily_params:
                print(f"{daily_params} only available in daily intervals")

            #append time interval for rasterizer filename generation
            hourly_task_map = [[a, b, "hourly"] for a, b in product(hour_list, hourly_params)]
            daily_task_map = [[a, b, "daily"] for a, b in product(day_list, daily_params)]

            task_map = hourly_task_map + daily_task_map

            if not task_map:
                raise ValueError("No tasks could be mapped — check date list and parameter selections.")

        else: 
            raise Exception(f"TaskMapper only accepts 'daily' or 'hourly', got {self.temporal_resolution}")
            
        return task_map
    

    def generate_date_list(self, date_from: str, date_to: str, interval: str):
        """
        Generate list of dates in API-compatible format within the range.

        Parameters:
            date_from (str): Start date in DDMMYYYY
            date_to (str): End date in DDMMYYYY
            interval (str): 'daily' or 'hourly'

        Returns:
            list of ISO 8601 time range strings
        """
        start_date = datetime.strptime(date_from, "%d%m%Y")
        end_date = datetime.strptime(date_to, "%d%m%Y")
        date_list = []

        if interval == "daily":
            current_date = start_date
            while current_date <= end_date:
                date_str = f"{current_date.strftime('%Y-%m-%dT00:00:00Z')}/{current_date.strftime('%Y-%m-%dT23:59:00Z')}"
                date_list.append(date_str)
                current_date += timedelta(days=1)

        elif interval == "hourly":
            current = datetime.combine(start_date, datetime.min.time())
            end = datetime.combine(end_date, datetime.max.time())
            while current <= end:
                hour_start = current.strftime('%Y-%m-%dT%H:00:00Z')
                hour_end = (current + timedelta(minutes=59, seconds=59)).strftime('%Y-%m-%dT%H:59:59Z')
                date_list.append(f"{hour_start}/{hour_end}")
                current += timedelta(hours=1)

        else:
            raise ValueError("Interval must be 'daily' or 'hourly'")

        return date_list

    

    def get_parameter_temp_res(self):
    # Parameter resolution lookup table
        parameter_resolution = {
            "mean_temp": ["hourly", "daily"],
            "mean_daily_max_temp": ["daily"],
            "max_temp_w_date": ["hourly", "daily"],
            "no_ice_days": ["daily"],
            "no_summer_days": ["daily"],
            "mean_daily_min_temp": ["daily"],
            "min_temp": ["hourly", "daily"],
            "no_cold_days": ["daily"],
            "no_frost_days": ["daily"],
            "no_tropical_nights": ["daily"],
            "no_lightning_strikes": ["hourly", "daily"],
            "acc_heating_degree_days_17": ["daily"],
            "mean_relative_hum": ["hourly", "daily"],
            "drought_index": ["daily"],
            "pot_evaporation_makkink": ["daily"],
            "mean_wind_speed": ["hourly", "daily"],
            "max_wind_speed_10min": ["hourly", "daily"],
            "max_wind_speed_3sec": ["hourly", "daily"],
            "mean_wind_dir": ["hourly", "daily"],
            "mean_pressure": ["hourly", "daily"],
            "bright_sunshine": ["hourly", "daily"],
            "mean_radiation": ["hourly", "daily"],
            "acc_precip": ["hourly", "daily"],
            "no_days_acc_precip_01": ["daily"],
            "no_days_acc_precip_1": ["daily"],
            "no_days_acc_precip_10": ["daily"],
            "max_precip_30m": ["daily"],
            "mean_cloud_cover": ["daily"],
            "snow_depth": ["daily"],
            "temp_grass": ["daily"],
            "temp_soil_10": ["daily"],
            "temp_soil_30": ["daily"],
            "leaf_moisture": ["daily"],
            "vapour_pressure_deficit_mean": ["daily"]
        }

        return parameter_resolution
