from json_utils import DMIJSONUtils

import os
import sys
import numpy as np
import rasterio as rio
from rasterio.transform import from_origin
from rasterio.transform import rowcol
from rasterio.warp import transform_bounds


class Rasterizer:
    """
    Tools for making and localizing ET data

    Parameters:
     - et_files (list): list of geotiffs of evaporative fraction (ETF)
     - dmi_param (str, optional): parameter in DMI climate data to apply to ETF data. Defaults to "pot_evaporation_makkink"
     - api_output (str path): path to output directory
     - crs (crs str, optional): crs of output rasters. Defaults to EPSG:4326
    """

    def __init__(self, climate_data, raster_dir, crs = "EPSG:4329", time_interval = 'daily'):

        self.climate_data = climate_data
        self.raster_dir = raster_dir
        self.crs = crs
        self.time_interval = time_interval


    def build_raster(self):
        """
        This script converts the DMI climate grid data from txt description to raster
        """

        raster_path = self.build_raster_path()

        self.create_blank_raster(raster_path)
        raster_instruction_set = self.climate_data_to_instruction_set()
    
        self.write_climate_data_to_raster(raster_path, raster_instruction_set)

        return raster_path


    def build_raster_path(self):
        """
        Takes a string like '2020-01-10T00:00:00Z'
        and returns 2020_01_10T00_00_00Z'
        Used to convert time ranges to path friendly strings
        """

        json_string = self.climate_data[0]

        # Timestamps being processed: 2020-01-02T00:00:00.001000+01:00
        timestamp = DMIJSONUtils.get_from_time(json_string)
        timestamp = timestamp.replace('-', '_').replace(':', '_')

        if self.time_interval == "daily":
            timestamp = timestamp[:16]
        elif self.time_interval == "hourly": 
            timestamp = timestamp[:10]
        else:
            raise ValueError(f"Interval must be 'daily' or 'hourly', recieved {self.time_interval}")
        
        climate_parameter = DMIJSONUtils.get_parameter_id(json_string)

        parameter_subdir = os.path.join(self.raster_dir, climate_parameter)
        os.makedirs(parameter_subdir, exist_ok = True)
        return os.path.join(parameter_subdir, timestamp + '.tif')


    def create_blank_raster(
            self,
            climate_raster_path: str,
            pixel_size: int = 10000, #matches DMI 10km grid
            crs: str = "EPSG:25832",
            dtype: str = "float32",
            fill_value: float = np.nan,
            compress: str = "lzw"
        ):
        """
        Creates a blank 10km resolution raster over Denmark using EPSG:25832.

        Parameters:
        - climate_raster_path: path to save the GeoTIFF
        - pixel_size: pixel resolution in meters (default 10,000 for 10km)
        - crs: coordinate reference system (default: EPSG:25832)
        - dtype: raster data type (default: float32)
        - fill_value: value to fill the raster with (default: NaN)
        - compress: compression method (default: 'lzw')
        """
        # DKN 100km grid corners (https://www.dst.dk/Site/Dst/SingleFiles/GetArchiveFile.aspx?fi=4890769105&fo=0&ext=kundecenter)
        # with 20km padding on each direction
        west, south = 380000, 5980000
        east, north = 920000, 6420000

        width = int((east - west) / pixel_size)
        height = int((north - south) / pixel_size)

        transform = from_origin(west, north, pixel_size, pixel_size)
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
        Assumes bboxes are in EPSG:4326 and raster is in EPSG:25832.

        Parameters:
        - raster_path: path to an existing raster (must be writable)
        - raster_instruction_set: list of [bbox, value] where bbox = (minx, miny, maxx, maxy)
        """
        with rio.open(raster_path, 'r+') as dst:
            transform = dst.transform
            dst_crs = dst.crs
            dtype = dst.dtypes[0]

            for bbox, value in raster_instruction_set:
                # Transform bbox from EPSG:4326 to raster CRS (EPSG:25832)
                transformed_bbox = transform_bounds("EPSG:4326", dst_crs, *bbox, densify_pts=21)
                minx, miny, maxx, maxy = transformed_bbox

                # Compute center of the transformed bbox
                cx = (minx + maxx) / 2
                cy = (miny + maxy) / 2

                # Get corresponding row, col in raster
                row, col = rowcol(transform, cx, cy)

                # Check if within raster bounds
                if 0 <= row < dst.height and 0 <= col < dst.width:
                    data = np.array([[value]], dtype=dtype)
                    window = rio.windows.Window(col_off=col, row_off=row, width=1, height=1)
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

