import duckdb
from pyspark.sql import SparkSession, functions
import os
import csv
import io
import glob

file_names = []
for i in glob.glob("data/**/*.csv", recursive=True):
    # file_names.append(os.path.basename(i))
    file_names.append(i)

# JAVA_HOME = "/Users/KB/.jdks/corretto-1.8.0_382"
# os.environ["JAVA_HOME"] = JAVA_HOME
# spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()
# df = spark.read.csv(file_names[0], header=True)

def table_from_csv():
    duckdb.sql("CREATE TABLE IF NOT EXISTS Vehicles (VIN_1_to_10 STRING, "
               "County STRING, "
               "City STRING, "
               "State STRING, "
               "Postal_Code BIGINT, "
               "Model_Year BIGINT, "
               "Make STRING, "
               "Model STRING, "
               "Electric_Vehicle_Type STRING, "
               "Clean_Alternative_Fuel_Vehicle_CAFV_Eligibility STRING, "
               "Electric_Range BIGINT, "
               "Base_MSRP BIGINT,"
               "Legislative_District BIGINT, "
               "DOL_Vehicle_ID BIGINT, "
               "Vehicle_Location STRING, "
               "Electric_Utility STRING, "
               "Twenty_Twenty_Census_Tract BIGINT)")

    duckdb.sql("INSERT INTO Vehicles SELECT * FROM read_csv_auto('" + file_names[0] + "')")

def cars_per_city():
    duckdb.sql("SELECT City, count(Make) AS Cars_per_city "
               "FROM Vehicles WHERE Electric_Vehicle_Type = 'Battery Electric Vehicle (BEV)'"
               "GROUP BY City "
               "ORDER BY count(Make) DESC ").show()

def top_three_vehicles():
    duckdb.sql("SELECT Make AS Car, count(Make) AS Number "
               "FROM Vehicles WHERE Electric_Vehicle_Type = 'Battery Electric Vehicle (BEV)'"
               "GROUP BY Make "
               "ORDER BY count(Make) DESC "
               "LIMIT 3").show()

def best_car_per_postal_code():

    # this commented code can be used to check if the uncommented query below works
    # duckdb.sql("SELECT Postal_Code, Make, count(Make) "
    #            "FROM Vehicles WHERE Postal_Code = '98620' "
    #            "GROUP BY Postal_Code, Make ORDER BY count(Make) DESC ").show()

    duckdb.sql("SELECT DISTINCT Postal_Code, "
               "FIRST_VALUE(Make) OVER (PARTITION BY Postal_Code ORDER BY count(Make) DESC) AS Best_Car "
               "FROM Vehicles "
               "GROUP BY Postal_Code, Make").show()

def num_of_cars_per_year_to_parquet():
    duckdb.sql("COPY (SELECT Model_Year, count(Make) AS Cars_per_year "
               "FROM Vehicles WHERE Electric_Vehicle_Type = 'Battery Electric Vehicle (BEV)'"
               "GROUP BY Model_Year "
               "ORDER BY count(Make) DESC) TO 'num_of_cars_per_year' (FORMAT PARQUET, "
               "PARTITION_BY (Model_Year), OVERWRITE_OR_IGNORE)")



def main():

    table_from_csv()

    # console output
    cars_per_city()
    top_three_vehicles()
    best_car_per_postal_code()

    # .parquet output
    num_of_cars_per_year_to_parquet()

    pass

if __name__ == '__main__':
    main()
