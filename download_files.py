#!/usr/bin/env python3
""" Script to download the Himawari data files to do some analysis and exploration

See: https://aws.amazon.com/marketplace/pp/prodview-eu33kalocbhiw#resources

 The S3 bucket here is the NOAA mirror of the Japan Meteorological Agency (JMA) Himawari-8 and Himawari-9 data.
 The files I am interested in are the full disk (FLDK) images. I want some of thethe NIR/SWIR bands (band 4, 5, 6)
 and maybe the visible bands (band 1, 2, 3) as well.

Dataset information can be found here:
https://www.data.jma.go.jp/mscweb/en/himawari89/cloud_service/fig/HimawariCloud_Data_Set_Information.pdf

Note: There are official netCDF files available for download from JMA.

All data downloaded should be attributed as described in the linked documentation.

TB NOTE: I need to look into the segmentation if I want to use this supplied data.

"""

import os
import datetime as dt

from botocore import UNSIGNED
from botocore.config import Config
import boto3
from pathlib import Path
from botocore.client import BaseClient


EXAMPLE_DATE = dt.datetime(2024, 4, 2, 16, 0, 0)

# Himawari-8 data starts on the 7th of July 2015
# Himawari-9 data starts on the 28th of October 2022
HIMAWARI_8_BUCKET = "noaa-himawari8"
HIMAWARI_9_BUCKET = "noaa-himawari9"

# There is a set pattern to grab the data files.
OBJECT_NAME_PATTERN = ("AHI-L1b-FLDK/{year}/{month}/{day}/{hour}{minute}/"
    "HS_{satellite_code}_{year}{month}{day}_{hour}{minute}_{band}_FLDK_{resolution}_S0101.DAT.bz2")

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


# For the given date, construct a dictionary of the required files
def get_object_name_dict(download_datetime: dt.datetime, obs_band: int) -> dict:

    if obs_band <= 4:
        resolution = 'R10'
    else:
        resolution = 'R20'

    return {
        'year': download_datetime.strftime('%Y'),
        'month': download_datetime.strftime('%m'),
        'day': download_datetime.strftime('%d'),
        'hour': download_datetime.strftime('%H'),
        'minute': download_datetime.strftime('%M'),
        'satellite_code': 'H08' if download_datetime < dt.datetime(2022, 10, 28) else 'H09',
        'band': f'B{obs_band:02d}',
        'resolution': resolution,
        'bucket_name': HIMAWARI_8_BUCKET if download_datetime < dt.datetime(2022, 10, 28) else HIMAWARI_9_BUCKET
    }


def download_files_for_date(download_datetime: dt.datetime, s3_client: BaseClient) -> None:
    """ For the given date, download the 6 required band files. """
    for band in range(1, 7):
        object_name_dict = get_object_name_dict(download_datetime, band)
        object_name = OBJECT_NAME_PATTERN.format(**object_name_dict)

        output_file_name = DATA_DIR / os.path.basename(object_name)
        if not os.path.exists(output_file_name):
            print(f"Downloading {object_name}")
            with open(output_file_name, 'wb') as f:
                s3_client.download_fileobj(object_name_dict['bucket_name'],
                                           object_name, f)

if __name__ == "__main__":
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    download_files_for_date(EXAMPLE_DATE, s3)
