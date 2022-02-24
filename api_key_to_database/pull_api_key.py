import requests
import sys
from datetime import datetime
from config_pull_api_key import host_postgre, database_postgre, user_postgre, password_postgre
import psycopg2

def delete_data_date(data_date):
        conn_postgre = psycopg2.connect(
                    host = host_postgre,
                    database = database_postgre,
                    user = user_postgre,
                    password = password_postgre
                        )

        cursor_postgre = conn_postgre.cursor()

        cursor_postgre.execute( \
                f"delete from cirebon \
                where last_updated = '{data_date}'")

        print("Data deleted")

        conn_postgre.commit()

        cursor_postgre.close()
        conn_postgre.close()

url = "https://weatherapi-com.p.rapidapi.com/current.json"

querystring = {"q":"-6.711220, 108.555176"}

headers = {
    'x-rapidapi-host': "weatherapi-com.p.rapidapi.com",
    'x-rapidapi-key': "51d777eb4emshdd6f2932378a8f9p1dea8bjsnc9d45deb5d23"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

data = response.json()

print(data)


last_updated = data["current"]["last_updated"]
last_updated = datetime.strptime(last_updated, '%Y-%m-%d %H:%M')

temp_c = data["current"]["temp_c"]
temp_f = data["current"]["temp_f"]
condition_text = data["current"]["condition"]["text"]
wind_mph = data["current"]["wind_mph"]
wind_kph = data["current"]["wind_kph"]
wind_degree = data["current"]["wind_degree"]
pressure_mb = data["current"]["pressure_mb"]
pressure_in = data["current"]["pressure_in"]
precip_mm = data["current"]["precip_mm"]
precip_in = data["current"]["precip_in"]
humidity = data["current"]["humidity"]
cloud = data["current"]["cloud"]
feelslike_c = data["current"]["feelslike_c"]
feelslike_f = data["current"]["feelslike_f"]
vis_km = data["current"]["vis_km"]
vis_miles = data["current"]["vis_miles"]
uv = data["current"]["uv"]
gust_mph = data["current"]["gust_mph"]
gust_kph = data["current"]["gust_kph"]


insert_data = (last_updated, \
                temp_c, \
                temp_f, \
                condition_text, \
                wind_mph, \
                wind_kph, \
                wind_degree, \
                pressure_mb, \
                pressure_in, \
                precip_mm, \
                precip_in, \
                humidity, \
                cloud, \
                feelslike_c, \
                feelslike_f, \
                vis_km, \
                vis_miles, \
                uv, \
                gust_mph, \
                gust_kph)

print(insert_data)

# for i in insert_data:
#     print(type(i))


delete_data_date(last_updated)


# insert data to postgre

conn_postgre = psycopg2.connect(
            host = host_postgre,
            database = database_postgre,
            user = user_postgre,
            password = password_postgre
                )

cursor_postgre = conn_postgre.cursor()

cursor_postgre.execute("INSERT INTO cirebon (last_updated, \
                                                temp_c, \
                                                temp_f, \
                                                condition_text, \
                                                wind_mph, \
                                                wind_kph, \
                                                wind_degree, \
                                                pressure_mb, \
                                                pressure_in, \
                                                precip_mm, \
                                                precip_in, \
                                                humidity, \
                                                cloud, \
                                                feelslike_c, \
                                                feelslike_f, \
                                                vis_km, \
                                                vis_miles, \
                                                uv, \
                                                gust_mph, \
                                                gust_kph)\
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, \
                                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
                                                , insert_data)

conn_postgre.commit()


cursor_postgre.close()
conn_postgre.close()


print("data is pulled by airflow on :", sys.argv[1])





# last_updated, \
#                 temp_c
#                 temp_f 
#                 condition_text 
#                 wind_mph 
#                 wind_kph 
#                 wind_degree 
#                 pressure_mb 
#                 pressure_in 
#                 precip_mm 
#                 precip_in 
#                 humidity 
#                 cloud 
#                 feelslike_c
#                 feelslike_f 
#                 vis_km 
#                 vis_miles 
#                 uv 
#                 gust_mph 
#                 gust_kph


# {
# "location":{
#     "name":"Cirebon"
#     "region":"West Java"
#     "country":"Indonesia"
#     "lat":-6.71
#     "lon":108.56
#     "tz_id":"Asia/Jakarta"
#     "localtime_epoch":1645689668
#     "localtime":"2022-02-24 15:01"
#     }
# "current":{
#     "last_updated_epoch":1645686000
#     "last_updated":"2022-02-24 14:00"  <==============1
#     "temp_c":29.7  <==============2
#     "temp_f":85.5  <==============3
#     "is_day":1
    
#     "condition":{
#     "text":"Light rain shower"  <==============4
#     "icon":"//cdn.weatherapi.com/weather/64x64/day/353.png"
#     "code":1240
#     }

#     "wind_mph":4.9  <==============5
#     "wind_kph":7.9  <==============6
#     "wind_degree":18  <==============7
#     "wind_dir":"NNE"
#     "pressure_mb":1009  <==============8
#     "pressure_in":29.78  <==============9
#     "precip_mm":1.1  <==============10
#     "precip_in":0.04  <==============11
#     "humidity":68  <==============12
#     "cloud":53  <==============13
#     "feelslike_c":34  <==============14
#     "feelslike_f":93.2  <==============15
#     "vis_km":10  <==============16
#     "vis_miles":6  <==============17
#     "uv":6  <==============18
#     "gust_mph":6.5  <==============19
#     "gust_kph":10.4  <==============20
# }
# }