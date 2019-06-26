from datetime import date, timedelta, datetime
from io import StringIO
import requests
import time
import pandas as pd
import json
import os
from keboola import docker # pro komunikaci s parametrama a input/output mapping
import warnings

# Parameters
data_folder = '/data/'


warnings.filterwarnings("ignore", message="numpy.dtype size changed")

print("Python libraries loaded.")

print(f"Current Working Directory is ... {os.getcwd()}")
print(f"Config taken from ... {data_folder}")



# # initialize KBC configuration
cfg = docker.Config(data_folder)
parameters = cfg.get_parameters()


def unix_times(start, end):
    dates = list(pd.date_range(start=start, end=end, freq='D'))
    # print(dates)
    dates.append(dates[-1] + timedelta(days=1))
    # print(dates)
    date_timetuples = [date_.timetuple() for date_ in dates]
    # print(date_timetuples)
    unix_dates = list(map(str, map(int, map(time.mktime, date_timetuples))))
    return dates, unix_dates

def validate(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD")


if __name__ == '__main__':
    login = parameters.get('Shop_id')
    password = parameters.get('Password')

    shop_name = parameters.get('Eshop_name')
    date_preset = parameters.get('Date_preset')
    default_start = parameters.get('Date_from')
    default_end = parameters.get('Date_to')


    start_dates = {'Yesterday': date.today() - timedelta(1),
                   'last_3_days': date.today() - timedelta(3),
                   'last_week': date.today() - timedelta(7),
                   'last_31_days': date.today() - timedelta(31),
                   'last_50_days': date.today() - timedelta(50),
                   'SPECIFIC_DATE': datetime.strptime(default_start, '%Y-%m-%d')}

    start_date = start_dates.get(date_preset, None)
    end_date = date.today() - timedelta(1)

    if date_preset =='SPECIFIC_DATE':
        start_date, end_date = validate(default_start), validate(default_end)
        start_date, end_date = default_start, default_end

    print(f"Costs loading from {start_date} to {end_date}")

    auth_tuple = (login, password)
    dates, unix_dates = unix_times(start_date, end_date)
    date_to = unix_dates[0]
    df = pd.DataFrame()
    request_ids = []

    for date_from in unix_dates[1:]:
        date_from, date_to = date_to, date_from

        time.sleep(4)

        req_id_response = requests.post(
            f"https://api.zbozi.cz/v1/shop/statistics/item?timestampFrom={date_from}&timestampTo={date_to}&dataFormat=csv",
            auth=auth_tuple)
        request_id = json.loads(req_id_response.text)['data']['requestId']
        request_ids.append(request_id)

    print(f"Request id's' are: {request_ids}")
    time.sleep(300)

    for date_, request_id in zip(dates, request_ids):
        stats = requests.get(f"https://api.zbozi.cz/v1/shop/statistics/item/csv?requestId={request_id}", auth=auth_tuple)
        stats.encoding = 'utf-8'
        io_data = StringIO(stats.text)
        daily_df = pd.read_csv(io_data, sep=";")
        daily_df['zobrazeni'] = daily_df['views (search)'] + daily_df['views (topProductDetail)'] + \
                                daily_df['views (productDetail)'] + daily_df['views (categoryListing)'] + \
                                daily_df['views (categorySearch)']

        daily_df['prokliky'] = daily_df['clicks (search)'] + daily_df['clicks (topProductDetail)'] + \
                               daily_df['clicks (productDetail)'] + daily_df['clicks (categoryListing)'] + \
                               daily_df['clicks (categorySearch)']

        daily_df['celkova_cena_za_prokliky'] = daily_df['cost (search)'] + daily_df['cost (topProductDetail)'] + \
                                               daily_df['cost (productDetail)'] + daily_df['cost (categoryListing)'] + \
                                               daily_df['cost (categorySearch)']

        daily_df['pocet_konverzi'] = daily_df['conversions (search)'] + daily_df['conversions (topProductDetail)'] + \
                                     daily_df['conversions (productDetail)'] + daily_df['conversions (categoryListing)'] + \
                                     daily_df['conversions (categorySearch)']

        daily_df.rename(columns={'itemId': 'id_polozky', 'itemTitle': 'jmeno_polozky'}, inplace=True)
        daily_df['date'] = date_
        daily_df['eshop_name'] = shop_name
        daily_df = daily_df[['id_polozky', 'jmeno_polozky', 'zobrazeni', 'prokliky',
                             'celkova_cena_za_prokliky', 'pocet_konverzi', 'date','eshop_name']]
        df = pd.concat([df, daily_df])
        time.sleep(60)

    df.to_csv(f'{data_folder}out/tables/final.csv', index=False)
