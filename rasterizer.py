from json_utils import DMIJSONUtils

import os
import sys
import numpy as np
import rasterio as rio
from rasterio.transform import from_origin
from rasterio.windows import from_bounds


class Rasterizer:
    """
    Tools for making and localizing ET data

    Parameters:
     - et_files (list): list of geotiffs of evaporative fraction (ETF)
     - dmi_param (str, optional): parameter in DMI climate data to apply to ETF data. Defaults to "pot_evaporation_makkink"
     - api_output (str path): path to output directory
     - crs (crs str, optional): crs of output rasters. Defaults to EPSG:4326
    """

    def __init__(self, climate_data, raster_dir, crs = "EPSG:4329"):

        self.climate_data = climate_data
        self.raster_dir = raster_dir
        self.crs = crs


    def build_raster(self, include_hour = False):
        """
        This script converts the DMI climate grid data from txt description to raster
        """

        raster_path = self.build_raster_path(include_hour)

        self.create_blank_raster(raster_path)
        raster_instruction_set = self.climate_data_to_instruction_set()
    
        self.write_climate_data_to_raster(raster_path, raster_instruction_set)

        return raster_path


    def build_raster_path(self, include_hour = False):
        """
        Takes a string like '2020-01-10T00:00:00Z'
        and returns 2020_01_10T00_00_00Z'
        Used to convert time ranges to path friendly strings
        """

        json_string = self.climate_data[0]

        # Timestamps being processed: 2020-01-02T00:00:00.001000+01:00
        timestamp = DMIJSONUtils.get_from_time(json_string)
        timestamp = timestamp.replace('-', '_').replace(':', '_')

        if include_hour:
            timestamp = timestamp[:15]
        else:
            timestamp = timestamp[:10]
        
        climate_parameter = DMIJSONUtils.get_parameter_id(json_string)

        parameter_subdir = os.path.join(self.raster_dir, climate_parameter)
        os.makedirs(parameter_subdir, exist_ok = True)
        return os.path.join(parameter_subdir, timestamp + '.tif')


    def create_blank_raster(
            self,
            climate_raster_path: str,
            bbox: tuple = (7.00, 53.00, 16.00, 59.00), #dk bbbox
            resolution_deg: float = 0.0003,
            crs: str = "EPSG:4326",
            dtype: str = "float32",
            fill_value: float = np.nan,
            compress: str = "lzw"
        ):
            """
            Creates a blank raster over the specified bounding box with given resolution.

            Parameters:
            - output_path: path to save the GeoTIFF
            - bbox: (west, south, east, north) in degrees
            - resolution_deg: cell size in degrees (approx. 30 m ~ 0.00027°)
            - crs: coordinate reference system (default: EPSG:4326)
            - dtype: raster data type (default: float32)
            - fill_value: value to fill the raster with (default: NaN)
            - compress: compression method (default: 'lzw')
            """
            west, south, east, north = bbox
            width = int((east - west) / resolution_deg)
            height = int((north - south) / resolution_deg)
            transform = from_origin(west, north, resolution_deg, resolution_deg)
            data = np.full((height, width), fill_value, dtype=dtype)

            with rio.open(
                climate_raster_path,
                "w",
                driver="GTiff",
                height=height,
                width=width,
                count=1,
                dtype=dtype,
                crs=crs,
                transform=transform,
                nodata=fill_value,
                compress=compress
            ) as dst:
                dst.write(data, 1)

    def climate_data_to_instruction_set(self):

        instruction_set = []
        for json in self.climate_data:
            coords = DMIJSONUtils.get_bbox(json)[0]

            lons = [pt[0] for pt in coords]
            lats = [pt[1] for pt in coords]
            bbox = [min(lons), min(lats), max(lons), max(lats)]
        
            value = DMIJSONUtils.get_value(json)

            instruction_set.append([bbox, value])

        return instruction_set
    

    def write_climate_data_to_raster(self, raster_path: str, raster_instruction_set: list):
        """
        Writes values to an existing raster given a list of [bbox, value] entries.
        
        Parameters:
        - raster_path: path to an existing raster (must be writable)
        - bbox_value_list: list of [bbox, value] where bbox = (minx, miny, maxx, maxy)
        """
        with rio.open(raster_path, 'r+') as dst:
            transform = dst.transform
            dtype = dst.dtypes[0]

            for bbox, value in raster_instruction_set:
                minx, miny, maxx, maxy = bbox
                # Get the window (pixel region) corresponding to the bbox
                window = from_bounds(minx, miny, maxx, maxy, transform=transform)
                window = window.round_offsets().round_lengths()
                
                # Create a matching array filled with the value
                data = np.full((int(window.height), int(window.width)), value, dtype=dtype)
                
                # Write to band 1
                dst.write(data, 1, window=window)


    def smooth_nodata_pixels(self):
        with rio.open(self.output_path, 'r+') as dst:
            data = dst.read(1)
            nodata_value = dst.nodata
            height, width = data.shape

            smoothed_data = data.copy()

            # Offsets to get the neighboring pixels
            offsets = [(-1, 0), (1, 0), (0, -1), (0, 1),  # direct neighbors
                    (-1, -1), (-1, 1), (1, -1), (1, 1)]  # diagonal neighbors

            for y in range(1, height - 1):
                for x in range(1, width - 1):
                    if data[y, x] == nodata_value:

                        neighbor_values = []
                        for dy, dx in offsets:
                            neighbor_val = data[y + dy, x + dx]
                            if neighbor_val != nodata_value:
                                neighbor_values.append(neighbor_val)

                        if len(neighbor_values) >= 2:
                            smoothed_data[y, x] = np.mean(neighbor_values)

            dst.write(smoothed_data, 1)

