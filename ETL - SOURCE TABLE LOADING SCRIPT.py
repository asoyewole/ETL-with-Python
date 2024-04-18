# Table Upload Script


# import required libraries
import mysql.connector
from datetime import datetime
import csv


# Establish a database connection
cnx = mysql.connector.connect(
    host = 'localhost',
    user = 'root',
    database = 'assignment4',
    password = 'datascience',
)
print('Successfully Connected to the Database!')

cursor = cnx.cursor()

current_time = datetime.now()


def s1_loader(filename, sql_load_statement):
    # Function for loading all 3 databases with the csv files
    try:
        counter = 0
        #open and read the content of csv into csvReader with delimeter ","
        with open(filename, newline = '') as csvData:
            csvReader = csv.reader(csvData, delimiter = ',')
            # take out the headers and save in a seperate variable
            headers = next(csvReader)
            # parse the remaining, each row to a list
            list_csv = []
            for row in csvReader:
                list_csv.append(row[:])
            # change all items with no entries to zero
            for line in list_csv:
                for item in line:
                    if item =='':
                        item_index = line.index(item)
                        line[item_index] = 0
            # load the list into the database            
            for row in list_csv:
                cursor.execute(sql_load_statement, row)
                counter+=1
        cnx.commit()
        print(counter, ' Records processed successfully')
    except:
        print('insertion failed after ', counter, ' entries')

# csv file names as variables for the function
file_name_1 = 'cropyield.csv'
file_name_2 = 'fertilizers.csv'
file_name_3 = 'pesticide-use-per-hectare-of-cropland.csv'

#sql statements as variables for the function
sql_load_statement_1 = '''
        INSERT INTO source_crop_yield (
        Entity, Year, barley_attainable, cassava_attainable, 
        cotton_attainable, groundnut_attainable, maize_attainable, millet_attainable, oilpalm_attainable, potato_attainable, rapeseed_attainable, rice_attainable, rye_attainable, sorghum_attainable, soybean_attainable, sugarbeet_attainable, sugarcane_attainable, sunflower_attainable, wheat_attainable, wheat_yield_gap, barley_yield_gap, rye_yield_gap, millet_yield_gap, sorghum_yield_gap, maize_yield_gap, cassava_yield_gap, soybeans_yield_gap, rapeseed_yield_gap, sugarbeet_yield_gap, sugarcane_yield_gap, potato_yield_gap, oilpalm_yield_gap, groundnut_yield_gap, rice_yield_gap, sunflower_yield_gap, cotton_yield_gap, Inserted_By, Inserted_Date) 
                    
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, 'Ayotunde Oyewole', current_time)'''

sql_load_statement_2 = '''
        INSERT INTO source_fertilizers(Entity, Country_Code, Year, Fertilizer_Consumption, Inserted_By, Inserted_Date)

        VALUES(%s,%s,%s,%s, 'Ayotunde Oyewole', current_time)
'''
sql_load_statement_3 = '''
        INSERT INTO source_pesticide_use(Entity, Country_Code, Year, Pesticide_Indicators, Inserted_By, Inserted_Date)

        VALUES(%s,%s,%s,%s, 'Ayotunde Oyewole', current_time)
'''
# Call the function to load each db table
s1_loader(file_name_1, sql_load_statement_1)
s1_loader(file_name_2, sql_load_statement_2)
s1_loader(file_name_3, sql_load_statement_3)
