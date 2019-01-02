# pems_crawler
PeMS crawler

This is a crawler which can download stations' information and time series data of each station from PeMS.

The file "BayStations.txt" contains all of stations' id of Bay Area. You can run this program to download all stations' information and their time series data.

You can change the last line of crawler.py to change start time and end time of the time series you want to download.

## Requirements
python3

+ requests

use `pip install -r requirements.txt` to install dependencies.

## Usage

```
python crawler.py
```