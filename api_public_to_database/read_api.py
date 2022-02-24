# get data from API
from config_read_api import driver_sqlserver, server_sqlserver, database_sqlserver, uid_sqlserver, pwd_sqlserver
from config_read_api import host_postgre, database_postgre, user_postgre, password_postgre, database_postgre_index
from config_read_api import table_daily_cases_sqlserver
from datetime import datetime
import pyodbc
import psycopg2
import requests


# checking date of data
def checking_data_date():
    # checking data di table rows information Postgre (output)
    conn_postgre = psycopg2.connect(
            host = host_postgre,
            database = database_postgre_index,
            user = user_postgre,
            password = password_postgre
                )

    cursor_postgre = conn_postgre.cursor()

    cursor_postgre.execute("SELECT last_date from quantity_rows_table where table_name = 'daily_cases' and database_type = 'sqlserver'")
    last_date = cursor_postgre.fetchall()
    last_date = last_date[0][0]
    print("Last date that has recieved =", last_date)

    conn_postgre.commit()

    cursor_postgre.close()
    conn_postgre.close()
    return last_date

def delete_data_date(penambahan_tanggal):
        conn_sqlserver = pyodbc.connect(
                f'DRIVER={driver_sqlserver}; \
                SERVER={server_sqlserver}; \
                DATABASE={database_sqlserver}; \
                UID={uid_sqlserver}; \
                PWD={pwd_sqlserver}')
        
        cursor_sqlserver = conn_sqlserver.cursor()


        cursor_sqlserver.execute( \
                f"delete from daily_cases \
                where penambahan_tanggal = '{penambahan_tanggal}'")

        print("Data deleted")

        conn_sqlserver.commit()

        cursor_sqlserver.close()
        conn_sqlserver.close()


# insert data to sql server

def insert_data_to_sqlserver():
        conn_sqlserver = pyodbc.connect(
                f'DRIVER={driver_sqlserver}; \
                SERVER={server_sqlserver}; \
                DATABASE={database_sqlserver}; \
                UID={uid_sqlserver}; \
                PWD={pwd_sqlserver}')
        
        cursor_sqlserver = conn_sqlserver.cursor()


        cursor_sqlserver.execute( \
                f'insert into daily_cases \
                (penambahan_jumlah_positif, \
                penambahan_jumlah_meninggal, \
                penambahan_jumlah_sembuh, \
                penambahan_jumlah_dirawat, \
                penambahan_tanggal, \
                penambahan_created, \
                total_jumlah_positif, \
                total_jumlah_dirawat, \
                total_jumlah_sembuh, \
                total_jumlah_meninggal) \
                values  {insert_data}')

        print("Data from API has inserted to SQL Server")

        conn_sqlserver.commit()

        cursor_sqlserver.close()
        conn_sqlserver.close()

# Update date information
def update_data_date(new_date):
    conn_postgre = psycopg2.connect(
            host = host_postgre,
            database = database_postgre_index,
            user = user_postgre,
            password = password_postgre
                )

    cursor_postgre = conn_postgre.cursor()


    cursor_postgre.execute(f"UPDATE quantity_rows_table  SET last_date = '{new_date}' WHERE table_name = 'daily_cases' and database_type = 'sqlserver';")
    conn_postgre.commit()

    cursor_postgre.close()
    conn_postgre.close()

    print("Updated last_date of pulling data from API to SQL Server")



# Start pull data
response = requests.get("https://data.covid19.go.id/public/api/update.json")
print(response)


data = response.json()

# print("1. Data")
# print(data["data"])

print("2. Update penambahan")
print(data["update"]["penambahan"])

# print("3. Update harian")
# print(data["update"]["harian"])

print("4. Update total")
print(data["update"]["total"])

# store in each variable
penambahan_jumlah_positif = data["update"]["penambahan"]["jumlah_positif"]
penambahan_jumlah_meninggal = data["update"]["penambahan"]["jumlah_meninggal"]
penambahan_jumlah_sembuh = data["update"]["penambahan"]["jumlah_sembuh"]
penambahan_jumlah_dirawat = data["update"]["penambahan"]["jumlah_dirawat"]
penambahan_tanggal = data["update"]["penambahan"]["tanggal"]
penambahan_created = data["update"]["penambahan"]["created"]

total_jumlah_positif = data["update"]["total"]["jumlah_positif"]
total_jumlah_dirawat = data["update"]["total"]["jumlah_dirawat"]
total_jumlah_sembuh = data["update"]["total"]["jumlah_sembuh"]
total_jumlah_meninggal = data["update"]["total"]["jumlah_meninggal"]

insert_data = (penambahan_jumlah_positif, \
                penambahan_jumlah_meninggal, \
                penambahan_jumlah_sembuh, \
                penambahan_jumlah_dirawat, \
                penambahan_tanggal, \
                penambahan_created, \
                total_jumlah_positif, \
                total_jumlah_dirawat, \
                total_jumlah_sembuh, \
                total_jumlah_meninggal)



# last_date = checking_data_date()


penambahan_tanggal_datetype = datetime.strptime(penambahan_tanggal, '%Y-%m-%d').date()


print(str(penambahan_tanggal))


# start input data
print("INPUT")

# delete data berdasarkan penambahan tanggal
delete_data_date(penambahan_tanggal)


insert_data_to_sqlserver()

# update_data_date(str(penambahan_tanggal))

print("Data inputed")


