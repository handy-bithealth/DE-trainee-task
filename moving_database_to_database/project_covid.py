import psycopg2
import pyodbc
from config_project_covid import driver_sqlserver, server_sqlserver, database_sqlserver, uid_sqlserver, pwd_sqlserver
from config_project_covid import host_postgre, database_postgre, user_postgre, password_postgre, database_postgre_index


def checking_output_data():
    # checking data di Postgre (output)
    conn_postgre = psycopg2.connect(
            host = host_postgre,
            database = database_postgre_index,
            user = user_postgre,
            password = password_postgre
                )

    cursor_postgre = conn_postgre.cursor()

    cursor_postgre.execute("SELECT number_of_rows from quantity_rows_table where table_name = 'covid_variants'")
    count_data_output = cursor_postgre.fetchall()
    count_data_output = count_data_output[0][0]
    print("Data di tempat tujuan / PostgreSQL = ", count_data_output)

    conn_postgre.commit()

    cursor_postgre.close()
    conn_postgre.close()
    return count_data_output


def checking_input_data():
    #checking input data
    conn_sqlserver = pyodbc.connect(
        f'DRIVER={driver_sqlserver}; \
        SERVER={server_sqlserver}; \
        DATABASE={database_sqlserver}; \
        UID={uid_sqlserver}; \
        PWD={pwd_sqlserver}')
    
    cursor_sqlserver = conn_sqlserver.cursor()

    cursor_sqlserver.execute("SELECT max(id) from covid_variants")
    count_data_input = cursor_sqlserver.fetchall()
    count_data_input = count_data_input[0][0]

    cursor_sqlserver.close()
    conn_sqlserver.close()

    print("Data input =", count_data_input)
    return count_data_input

def read_data_sqlserver(count_data_output):
    #read data from SQL Server
    conn_sqlserver = pyodbc.connect(
        f'DRIVER={driver_sqlserver}; \
        SERVER={server_sqlserver}; \
        DATABASE={database_sqlserver}; \
        UID={uid_sqlserver}; \
        PWD={pwd_sqlserver}')
    
    cursor_sqlserver = conn_sqlserver.cursor()

    cursor_sqlserver.execute( \
        f"SELECT id, location, date_covid, variant, \
        num_sequences, perc_sequences, num_sequences_total \
        FROM dbo.covid_variants \
        where id > {count_data_output}")

    data_input_sqlserver = cursor_sqlserver.fetchall()

    cursor_sqlserver.close()
    conn_sqlserver.close()

    #insert data to postgre
    data_input = data_input_sqlserver
    print("Rows of data that needed to inserted ", len(data_input_sqlserver))
    return data_input

def input_data_postgre(data_input):
    
    conn_postgre = psycopg2.connect(
        host = host_postgre,
        database = database_postgre,
        user = user_postgre,
        password = password_postgre
            )

    cursor_postgre = conn_postgre.cursor()


    inserted_rows = 0

    for i_data_input in data_input:
        cursor_postgre.execute('INSERT INTO covid_variants(id, \
            location, date_covid, variant, num_sequences, \
            perc_sequences, num_sequences_total) \
            VALUES (%s, %s, %s, %s, %s, %s, %s)' , i_data_input)
            
        conn_postgre.commit()

        inserted_rows += 1

    cursor_postgre.close()
    conn_postgre.close()

    print("Data inputed to postgre")

    return inserted_rows



def update_index_table(count_data_output, inserted_rows):
    conn_postgre = psycopg2.connect(
            host = host_postgre,
            database = database_postgre_index,
            user = user_postgre,
            password = password_postgre
                )

    cursor_postgre = conn_postgre.cursor()

    inserted_rows_fix =  count_data_output + inserted_rows

    cursor_postgre.execute(f"UPDATE quantity_rows_table  SET number_of_rows = {inserted_rows_fix} WHERE table_name = 'covid_variants';")
    conn_postgre.commit()

    cursor_postgre.close()
    conn_postgre.close()

    print("Updated index_table postgre")





count_data_output = checking_output_data()

count_data_input = checking_input_data()



# Conditional
if count_data_input > count_data_output:
    #input data
    print("INPUT the data in progress")
    data_input = read_data_sqlserver(count_data_output)
    
    inserted_rows = input_data_postgre(data_input)

    update_index_table(count_data_output, inserted_rows)
    

elif count_data_input == count_data_output:
    print("NO UPDATE")

else:
    print("DATA CORRUPTED, please check the source..")