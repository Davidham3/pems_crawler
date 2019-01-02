# -*- coding:utf-8 -*-

import os
import requests
from datetime import datetime
from datetime import timedelta
from urllib.parse import quote_plus
import concurrent.futures

headers = {
	'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',           
	'Accept-Encoding': 'gzip, deflate, sdch',
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
}

directory = os.path.dirname(os.path.abspath(__file__))

class spider:
    def __init__(self, username, password, proxies = False):
        self.home = "http://pems.dot.ca.gov/"
        self.s = requests.Session()
        if proxies:
            self.s.proxies = proxies
        self.username = username
        self.password = password

    def get(self, url):
        '''
        HTTP get request
        '''
        return self.s.get(url, headers = headers)

    def post(self, url, data):
        '''
        HTTP post request
        '''
        return self.s.post(url, data = data, headers = headers)

    def login(self):
        '''
        sign in PEMS
        '''
        print('try to login')
        data = {'username': self.username,
            'password': self.password,
            'login': 'Login',
            'redirect': ""}
        self.post(self.home, data)

    def download_station_time_series_5min(self, station_id, start_time, end_time, areaname):
        '''
        download each station's time series data, time interval is 5min

        Parameters
        ----------
        station_id: str, e.g. 402000

        start_time: str, %Y%m%d%H%M, e.g. 201701010000

        end_time: str, %Y%m%d%H%M, e.g. 201701012359
        '''
        print('try to download %s %s %s'%(station_id, start_time, end_time))

        s_time_id_f = datetime.strptime(start_time, "%Y%m%d%H%M").strftime("%m/%d/%Y+%H:%M")
        e_time_id_f = datetime.strptime(end_time, "%Y%m%d%H%M").strftime("%m/%d/%Y+%H:%M")
        
        delta = timedelta(days = 1)
        s_time_id = str(int(datetime.strptime(datetime.strptime(start_time, "%Y%m%d%H%M").strftime("%Y%m%d") + "0800",
                                    "%Y%m%d%H%M").timestamp()))
        e_time_id = str(int(datetime.strptime((datetime.strptime(end_time, "%Y%m%d%H%M") + delta).strftime("%Y%m%d") + "0759",
                                    "%Y%m%d%H%M").timestamp()))
        data_str = '''report_form=1
                        dnode=VDS
                        content=loops
                        tab=det_timeseries
                        export=text
                        station_id=405572
                        s_time_id=1483228800
                        s_time_id_f=01%2F01%2F2017+00%3A00
                        e_time_id=1483315140
                        e_time_id_f=01%2F01%2F2017+23%3A59
                        tod=all
                        tod_from=0
                        tod_to=0
                        dow_0=on
                        dow_1=on
                        dow_2=on
                        dow_3=on
                        dow_4=on
                        dow_5=on
                        dow_6=on
                        holidays=on
                        q=flow
                        q2=
                        gn=5min
                        agg=on'''
        data = dict(map(lambda x: x.strip().split('='), data_str.split('\n')))
        data['station_id'] = station_id
        data['s_time_id'] = s_time_id
        data['e_time_id'] = e_time_id
        data['s_time_id_f'] = s_time_id_f
        data['e_time_id_f'] = e_time_id_f

        url = self.home + '?' + '&'.join(map(lambda x: '='.join((x[0][0], quote_plus(x[0][1], safe = "+"))), zip(data.items())))
        response = self.get(url)

        if not os.path.exists(os.path.normpath(os.path.join(directory, 'time_series/%s/'%(areaname)))):
            os.makedirs(os.path.normpath(os.path.join(directory, 'time_series/%s/'%(areaname))))

        with open('%s%s_%s_%s.txt'%(os.path.normpath(os.path.join(directory, 'time_series/%s/'%(areaname))), station_id, start_time, end_time), 'w') as f:
            f.write(response.text)

    def download_station_metadata(self, station, areaname):
        print('try to download meta data of station %s'%(station))
        url = "%s?station_id=%s&dnode=VDS&content=sta_cfg"%(self.home, station)
        r = self.get(url)

        if not os.path.exists(os.path.normpath(os.path.join(directory, 'station_metadata/%s/'%(areaname)))):
            os.makedirs(os.path.normpath(os.path.join(directory, 'station_metadata/%s/'%(areaname))))

        with open("%s%s.html"%(os.path.normpath(os.path.join(directory, 'station_metadata/%s/'%(areaname))), station), "w") as f:
            f.write(r.text)

    def start(self, areaname, filename, start_time, end_time):
        '''
        Parameters
        ----------
        areaname: str, e.g. Bay

        filename: str, e.g. station.txt

        start_time, end_time: str, %Y%m%d

        '''
        # download all stations' metadata
        with open(filename, 'r') as f:
            station_list = f.read().strip().split('\n')
        
        if not os.path.exists(os.path.normpath(os.path.join(directory, 'station_metadata/%s'%(areaname)))):
            os.makedirs(os.path.normpath(os.path.join(directory, 'station_metadata/%s'%(areaname))))

        station_metadatas = os.listdir(os.path.normpath(os.path.join(directory, 'station_metadata/%s'%(areaname))))
        for station in station_list:
            if "%s.html"%(station) in station_metadatas:
                continue
            self.download_station_metadata(station, areaname)

        # compute timelist
        start_time = start_time + "0000"
        t = datetime.strptime(start_time, "%Y%m%d%H%M")
        delta = timedelta(days = 7)
        end_time = datetime.strptime(end_time, "%Y%m%d")
        timelist = []
        while t < end_time:
            tmp = [t.strftime("%Y%m%d") + "0000"]
            t = t + delta
            tmp.append((t - timedelta(days = 1)).strftime("%Y%m%d") + "2359")
            timelist.append(tmp)

        # compute all stations' time series start time and end time
        jobs = [(station, start_time, end_time) \
                for station in station_list \
                for start_time, end_time in timelist \
                if not os.path.exists(os.path.normpath(os.path.join(directory, 'time_series/%s/%s_%s_%s.txt'%(areaname, station, start_time, end_time))))]

        # multi threaded downloader
        with concurrent.futures.ThreadPoolExecutor(max_workers = 4) as executor:
            
            # add all jobs to future_to_url, {job: job's name}
            future_to_url = {executor.submit(self.download_station_time_series_5min, station, start_time, end_time, areaname): '_'.join((station, start_time, end_time)) for station, start_time, end_time in jobs}
            for future in concurrent.futures.as_completed(future_to_url):
                job_name = future_to_url[future]
                try:
                    future.result()
                except Exception as exc:
                    print('%r generated an exception: %s' % (job_name, exc))

if __name__ == "__main__":
    proxies = {'http': 'http://127.0.0.1:1080'}

    username = None
    password = None

    if username == None:
        username = input('please input your username of PeMS: ')

    if password == None:
        password = input('please input your password: ')

    # spider initialization
    a = spider()

    # sign in PEMS
    a.login(username, password, proxies = False)

    a.start('Bay', 'BayStations.txt', '20170101', '20170601')