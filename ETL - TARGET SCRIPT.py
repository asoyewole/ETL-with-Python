from datetime import datetime
import mysql.connector

# Create reading cursor
read_cnx = mysql.connector.connect(
host = 'localhost',
user = 'root',
database = 'assignment4',
password = 'datascience',
)
print("read connection successful!")


# Create reading cursor
insert_cnx = mysql.connector.connect(
host = 'localhost',
user = 'root',
database = 'assignment4',
password = 'datascience',
)
print("insert connection successful!")

select_cursor = read_cnx.cursor()
insert_cursor = insert_cnx.cursor()
 
current_time = datetime.now()
userid = 'Ayotunde Oyewole'
insert_counter = 0
read_counter = 0

sql_insert_statement = "insert into CerealCropYieldData (CountryCode, CountryDescription, Decade, IncompleteWarning, barleyActualYield, RyeActualYield, WheatActualYield, FertilizerKgHa, PesticideKgHa, Inserted_By, Inserted_Date) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s, %s)"

select_cursor.execute('''SELECT (CountryCode), (decade),  sum(Barley_actual_yield),
sum(rye_actual_yield), sum(wheat_actual_yield), sum(fertilizer_consumption),
sum(pesticide_indicators)
FROM 
tr_cerealcropyield group by Decade, countrycode, countrydescription order by countrycode''')

selectrow = select_cursor.fetchone()
while selectrow:
    read_counter +=1
    if selectrow[6] == None: IncompleteWarning = 'Y'
    elif selectrow[3] == None: IncompleteWarning = 'Y'
    else:IncompleteWarning = 'N'

    insert_data = selectrow[0], selectrow[1], IncompleteWarning, selectrow[2], selectrow[3], selectrow[4], selectrow[5], selectrow[6], userid, current_time

    insert_cursor.execute(sql_insert_statement, insert_data)
    insert_counter +=1
    selectrow = select_cursor.fetchone()

insert_cnx.commit()    



    



