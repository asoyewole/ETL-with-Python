# SOURCE TABLE CREATION SCRIPT


# import required libraries and dependencies
import mysql.connector



# Establish a database connection
cnx = mysql.connector.connect(
    host = 'localhost',
    user = 'root',
    database = 'assignment4',
    password = 'datascience',
)

print('Successfully Connected to the Database!')

cursor = cnx.cursor()


# #Create source table for crop yield
# try:
#     cursor.execute(
#             '''
#     CREATE TABLE source_crop_yield(
#     source_crop_yield_key int primary key auto_increment,
#     Entity varchar(50) not null,
#     Year int not null,
#     barley_attainable decimal(8,2),
#     cassava_attainable decimal(8,2), 
#     cotton_attainable decimal(8,2), 
#     groundnut_attainable decimal(8,2), 
#     maize_attainable decimal(8,2),
#     millet_attainable decimal(8,2),
#     oilpalm_attainable decimal(8,2),
#     potato_attainable decimal(8,2), 
#     rapeseed_attainable decimal(8,2),
#     rice_attainable decimal(8,2),
#     rye_attainable decimal(8,2),
#     sorghum_attainable decimal(8,2),
#     soybean_attainable decimal(8,2),
#     sugarbeet_attainable decimal(8,2),
#     sugarcane_attainable decimal(8,2),
#     sunflower_attainable decimal(8,2),
#     wheat_attainable decimal(8,2),
#     wheat_yield_gap decimal(8,2),
#     barley_yield_gap decimal(8,2),
#     rye_yield_gap decimal(8,2),
#     millet_yield_gap decimal(8,2),
#     sorghum_yield_gap decimal(8,2),
#     maize_yield_gap decimal(8,2), 
#     cassava_yield_gap decimal(8,2),
#     soybeans_yield_gap decimal(8,2),
#     rapeseed_yield_gap decimal(8,2),
#     sugarbeet_yield_gap decimal(8,2),
#     sugarcane_yield_gap decimal(8,2),
#     potato_yield_gap decimal(8,2),
#     oilpalm_yield_gap decimal(8,2),
#     groundnut_yield_gap decimal(8,2),
#     rice_yield_gap decimal(8,2),
#     sunflower_yield_gap decimal(8,2),
#     cotton_yield_gap decimal(8,2),
#     Inserted_By varchar (50),
#     Inserted_Date datetime
#     )
#     ''')

#     cnx.commit()

#     print("source_crop_yield table created successfully")
# except:
#     print('error creating crop_yield table')


# #Create source table for fertilizers
# try:
#     cursor.execute(
#         '''
#         CREATE TABLE source_fertilizers(
#         Fertilizer_key int primary key auto_increment,
#         Entity varchar(50) not null,
#         Country_Code varchar(10), 
#         Year smallint not null,
#         Fertilizer_Consumption decimal(8,2),
#         Inserted_By varchar (50),
#         Inserted_Date datetime
#         )
#     '''
#     )

#     cnx.commit()

#     print("source_fertilizers table created successfully")
# except:
#     print('error creating fertilizers table')


# #Create source table for pesticide use
# try:
#     cursor.execute(
#         '''
#         CREATE TABLE source_pesticide_use(
#         Source_pesticide_use_key int primary key auto_increment,
#         Entity varchar(50) not null, 
#         Country_Code varchar(10),
#         Year smallint not null,
#         Pesticide_Indicators decimal(7,2),
#         Inserted_By varchar (50),
#         Inserted_Date datetime
#         )
#     '''
#     )

#     cnx.commit()

#     print("source_pesticide_use table created successfully")
# except:
#     print('error creating source_pesticide_use')




# # Create transformation table
# try:
#     cursor.execute(
#         '''
#         CREATE TABLE tr_cerealcropyield(
#         crop_yield_key int primary key auto_increment,
#         CountryCode varchar(10) not null,
#         Decade smallint,
#         Year smallint not null,
#         IncompleteWarning char(1),
#         barley_attainable decimal(8,2), barley_yield_gap decimal(8,2), wheat_attainable  decimal(8,2), wheat_yield_gap  decimal(8,2), rye_attainable  decimal(8,2), rye_yield_gap  decimal(8,2), fertilizer_consumption  decimal(8,2),
#         Pesticide_Indicators decimal(8,2), barley_actual_yield decimal(8,2), wheat_actual_yield  decimal(8,2), rye_actual_yield  decimal(8,2),
#         Inserted_By varchar (50),
#         Inserted_Date datetime
#         )
#     '''
#     )

#     cnx.commit()

#     print("tr_cerealcropyield created successfully")
# except:
#     print('error creating tr_cerealcropyield table')


#Create target table
try:
    cursor.execute(
        '''
        CREATE TABLE CerealCropYieldData(
        CountryCode varchar(3),
        CountryDescription varchar(20),
        Decade smallint,
        IncompleteWarning char(1),
        barleyActualYield decimal(8,2), RyeActualYield  decimal(8,2), WheatActualYield  decimal(8,2), 
        FertilizerKgHa  decimal(8,2),
        PesticideKgHa decimal(8,2), 
        Inserted_By varchar (50),
        Inserted_Date date,
        primary key(Decade, CountryCode)
        )
    '''
    )

    cnx.commit()

    print("CerealCropYieldData table created successfully")
except:
    print('error creating CerealCropYieldData table')
