import mysql.connector
from datetime import datetime


# Establish a read-database connection
read_cnx = mysql.connector.connect(
    host='localhost',
    user='root',
    database='assignment4',
    password='datascience',
)

print("read connection successful!")

# Establish a write-database connection
write_cnx = mysql.connector.connect(
    host='localhost',
    user='root',
    database='assignment4',
    password='datascience',
)
print('write connection successful!')

# Declare the two cursors
selection_cursor = read_cnx.cursor()
insertion_cursor = write_cnx.cursor()

current_time = datetime.now()
insert_record_count = 0
read_count = 0

# Query the databases and join data
sql_select_statement = '''SELECT source_fertilizers.Country_Code, source_crop_yield.Year, source_crop_yield.barley_attainable, 
source_crop_yield.barley_yield_gap, source_crop_yield.wheat_attainable, source_crop_yield.wheat_yield_gap, 
source_crop_yield.rye_attainable, source_crop_yield.rye_yield_gap, source_fertilizers.Fertilizer_Consumption, source_pesticide_use.Pesticide_Indicators
FROM source_crop_yield 

LEFT JOIN source_fertilizers 
ON source_crop_yield.Entity= source_fertilizers.Entity
AND source_crop_yield.Year = source_fertilizers.Year

LEFT JOIN source_pesticide_use 
ON source_crop_yield.Entity= source_pesticide_use.Entity
AND source_crop_yield.Year = source_pesticide_use.Year

WHERE source_crop_yield.entity = "Canada" or source_crop_yield.entity = "Mexico" or source_crop_yield.entity = "United States"
'''

sql_insert_statement = '''INSERT INTO tr_cerealcropyield(
CountryCode,
        Year, Decade, IncompleteWarning,
        barley_attainable, barley_yield_gap, wheat_attainable, wheat_yield_gap, rye_attainable, rye_yield_gap, fertilizer_consumption,
        Pesticide_Indicators, barley_actual_yield, wheat_actual_yield, rye_actual_yield,
        Inserted_By,
        Inserted_Date
)
VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

'''


def yield_estimator(attainable, gap):
    actual_yield = float(attainable - gap)
    return actual_yield


selection_cursor.execute(sql_select_statement)

selectrow = selection_cursor.fetchone()
while selectrow:
    read_count += 1
    BarleyActualYield = yield_estimator(selectrow[2], selectrow[3])
    WheatActualYield = yield_estimator(selectrow[4], selectrow[5])
    RyeActualYield = yield_estimator(selectrow[6], selectrow[7])

    if 1960 <= selectrow[1] <= 1969:
        decade = 60
    elif 1970 <= selectrow[1] <= 1979:
        decade = 70
    elif 1980 <= selectrow[1] <= 1989:
        decade = 80
    elif 1990 <= selectrow[1] <= 1999:
        decade = 90
    elif 2000 <= selectrow[1] <= 2009:
        decade = 00
    elif 2010 <= selectrow[1] <= 2019:
        decade = 10

    for row in selectrow:
        if row == None or row == 0:
            IncompleteWarning = 'Y'
        else:
            IncompleteWarning = 'N'

    sql_insert_data = (selectrow[0], selectrow[1], decade, IncompleteWarning, selectrow[2], selectrow[3], selectrow[4], selectrow[5], selectrow[6],
                    selectrow[7], selectrow[8], selectrow[9], BarleyActualYield, WheatActualYield, RyeActualYield, 'Ayotunde Oyewole', current_time)

    insertion_cursor.execute(sql_insert_statement, sql_insert_data)
    insert_record_count += 1
    selectrow = selection_cursor.fetchone()
write_cnx.commit()
print(read_count, 'read successfully')
print(insert_record_count, " inserted successfully")
