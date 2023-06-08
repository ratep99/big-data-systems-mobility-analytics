from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col

# Method used to get cleaned dataset, ready for analysis

''' 
def clean_data(spark):
    # Read the denverVehicles.csv file
    vehicles_df = spark.read.csv("data/denverVehicles.csv", header=True, inferSchema=True)

    # Convert speed from m/s to km/h and rename columns
    vehicles_df = vehicles_df.withColumn("speed", round(vehicles_df["speed"] * 3.6, 2)) \
        .withColumnRenamed("speed", "speed_kmh") \
        .withColumnRenamed("x", "longitude") \
        .withColumnRenamed("y", "latitude")

    # Convert timestep to integer, cast the datetime column to timestamp, and add the timestep to the datetime
    vehicles_df = vehicles_df.withColumn("timestamp2", col("timestep").cast("integer")) \
        .withColumn("datetime", lit("2023-02-11 08:00:00").cast("timestamp"))

    vehicles_df = vehicles_df.withColumn("new_time",
                                         (F.unix_timestamp("datetime") + vehicles_df.timestamp2).cast('timestamp'))

    # Replace the timestep column with the new calculated time
    vehicles_df = vehicles_df.withColumn("timestep", vehicles_df["new_time"])
    vehicles_df = vehicles_df.withColumnRenamed("timestep", "timestamp")

    # Replace vehicle type values with simplified values (bus_bus -> bus, veh_passenger -> car)
    vehicles_df = vehicles_df.withColumn("type", \
                                         F.when(vehicles_df["type"] == "bus_bus", "bus"). \
                                         when(vehicles_df["type"] == "veh_passenger", "car"). \
                                         otherwise(vehicles_df["type"]))

    # Drop unneeded columns
    vehicles_df = vehicles_df.drop(
        *['posLat', 'slope', 'signals', 'datetime', 'timestamp2', 'new_time', 'angle'])

    # Reorder the columns in the dataframe
    vehicles_df = vehicles_df.select("timestamp", "id", "type", "latitude", "longitude",
                                     "speed_kmh", "acceleration", "distance", "odometer", "pos", "lane")

    vehicle_counts = vehicles_df.groupBy("id").agg(count("id").alias("count"))

    avg_appearance = vehicle_counts.agg(F.avg("count")).collect()[0][0]

    print("The average appearance of a vehicle in the dataframe is:", avg_appearance)
    # Show the resulting dataframe
    vehicles_df.show()
    print(vehicles_df.count())
    grouped_vehicles = vehicles_df.groupBy("id").count()
    vehicles_to_keep = grouped_vehicles.filter(col("count") >= 120)
    vehicles_to_keep = vehicles_to_keep.select("id")
    vehicles_df = vehicles_df.join(vehicles_to_keep, "id", "inner")
    print(vehicles_df.count())

    vehicles_df = vehicles_df.sort(vehicles_df["speed_kmh"])
    vehicles_df.show()

    # vehicles_df.coalesce(1).write.format("csv").option("header", "true").save("denverVehiclesCleaned.csv")
'''

DATA_PATH = "hdfs://namenode:9000/dir/denverVehiclesCleaned.csv"
SPARK_MASTER = "spark://spark-master:7077"
APP_NAME = "DenverMobility"
STATISTIC_CRITERIA = "speed_kmh"


def initialization():
    # Define Spark Configuration
    conf = SparkConf()
    conf.setMaster(SPARK_MASTER)
    spark_session = SparkSession.builder.config(conf=conf).appName(APP_NAME).getOrCreate()

    # Set the log level to ERROR to reduce the amount of output
    spark_session.sparkContext.setLogLevel("ERROR")
    data_frame = spark_session.read.csv(DATA_PATH, header=True, inferSchema=True)

    return spark_session, data_frame


def filter_vehicles_in_timespan(data_frame, start_time, end_time):
    return data_frame.filter((col("timestamp").between(start_time, end_time)))


def filter_vehicles_by_type_in_timespan(data_frame, vehicle_type, start_time, end_time):
    return data_frame.filter((col("type") == vehicle_type) & (col("timestamp").between(start_time, end_time)))


def filter_vehicles_in_location(data_frame, latitude_1, longitude_1, latitude_2, longitude_2):
    return data_frame.filter((col("latitude").between(latitude_1, latitude_2)) & (col("longitude").between(longitude_1, longitude_2)))


def filter_vehicles_by_type_in_location(data_frame, vehicle_type, latitude_1, longitude_1, latitude_2, longitude_2):
    return data_frame.filter((col("type") == vehicle_type) &
                             (col("latitude").between(latitude_1, latitude_2)) & (col("longitude").between(longitude_1, longitude_2)))


def filter_vehicles_by_type_in_timespan_and_location(data_frame, vehicle_type, start_time, end_time,
                                                     latitude_1, longitude_1, latitude_2, longitude_2):
    return data_frame.filter((col("type") == vehicle_type) &
                             (col("timestamp").between(start_time, end_time)) &
                             (col("latitude").between(latitude_1, latitude_2)) & (col("longitude").between(longitude_1, longitude_2)))


def calculate_statistics(data_frame, statistic_criteria):
    """
    Calculates statistics for a given DataFrame.

    Args:
        data_frame (DataFrame): The input DataFrame.
        statistic_criteria (str): The column name for which statistics are calculated.

    Returns:
        tuple: A tuple containing the calculated statistics in the order (mean, max, min, stddev).
    """
    # Calculate the statistics
    return data_frame.agg(
        mean(statistic_criteria),
        max(statistic_criteria),
        min(statistic_criteria),
        stddev(statistic_criteria)
    ).collect()[0]


def count_vehicles_above_threshold(data_frame: DataFrame, column: str, threshold: float) -> int:
    """
    Counts the number of vehicles that have a column value above a given threshold.

    Args:
        data_frame (DataFrame): The input DataFrame.
        column (str): The column name for the specified attribute.
        threshold (float): The column value threshold.

    Returns:
        int: The count of vehicles with column values above the threshold.
    """
    # Check if the data_frame parameter is a DataFrame
    if not isinstance(data_frame, DataFrame):
        raise TypeError("data_frame parameter must be a DataFrame")

    # Check if the column parameter is a string
    if not isinstance(column, str):
        raise TypeError("column parameter must be a string")

    # Check if the threshold parameter is a float
    if not isinstance(threshold, float):
        raise TypeError("threshold parameter must be a float")

    # Filter vehicles above the threshold
    high_attribute_vehicles = data_frame.filter(col(column) > threshold)

    # Count the distinct "vehicle_id" values
    count = high_attribute_vehicles.select("id").distinct().count()

    return count


def print_vehicles_above_threshold(data_frame: DataFrame, column: str, threshold: float) -> None:
    """
    Prints the attributes of vehicles that have a column value above a given threshold.

    Args:
        data_frame (DataFrame): The input DataFrame.
        column (str): The column name for the specified attribute.
        threshold (float): The column value threshold.

    Returns:
        None
    """
    # Check if the data_frame parameter is a DataFrame
    if not isinstance(data_frame, DataFrame):
        raise TypeError("data_frame parameter must be a DataFrame")

    # Check if the column parameter is a string
    if not isinstance(column, str):
        raise TypeError("column parameter must be a string")

    # Check if the threshold parameter is a float
    if not isinstance(threshold, float):
        raise TypeError("threshold parameter must be a float")

    # Filter vehicles above the threshold
    high_attribute_vehicles = data_frame.filter(col(column) > threshold)

    # Collect the attributes of high attribute vehicles
    attributes = high_attribute_vehicles.collect()
    print_step("Printing attributes of vehicles")
    # Print the attributes of high attribute vehicles
    for row in attributes:
        print(row.asDict())


def print_statistics(statistics):
    """
    Prints the statistics.

    Args:
        statistics (tuple): A tuple containing the statistics in the order (mean, max, min, stddev).
    """
    # Print the statistics
    print_step("Printing statistics")
    print("Mean: ", statistics[0])
    print("Max: ", statistics[1])
    print("Min: ", statistics[2])
    print("Standard Deviation: ", statistics[3])


def print_step(step):
    print("*******************************************************************************")
    print("----------------------------------- " + step + " -----------------------------------")
    print("*******************************************************************************")


# Main entry point of the application
if __name__ == '__main__':
    # Check the number of command-line arguments
    if len(sys.argv) < 3 | len(sys.argv) > 8:
        print("Usage: main.py <input folder> ")
        exit(-1)

    # Initialize Spark session and DataFrame
    spark, df = initialization()
    # Get the command-line arguments
    args = sys.argv
    
    if len(sys.argv) == 5:
        if "-" in sys.argv[1]:
            first_datetime = sys.argv[1] + " " + sys.argv[2]
            second_datetime = sys.argv[3] + " " + sys.argv[4]
            filtered_df = filter_vehicles_in_timespan(df, first_datetime, second_datetime)
        else:
            filtered_df = filter_vehicles_in_location(df, sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    elif len(sys.argv) == 6:
        if "-" in sys.argv[2]:
            first_datetime = sys.argv[2] + " " + sys.argv[3]
            second_datetime = sys.argv[4] + " " + sys.argv[5]
            filtered_df = filter_vehicles_by_type_in_timespan(df, sys.argv[1], first_datetime, second_datetime)
        else:
            filtered_df = filter_vehicles_by_type_in_location(df, sys.argv[1], sys.argv[2],
                                                              sys.argv[3], sys.argv[4], sys.argv[5])
    elif len(sys.argv) == 10:
        first_datetime = sys.argv[2] + " " + sys.argv[3]
        second_datetime = sys.argv[4] + " " + sys.argv[5]
        filtered_df = filter_vehicles_by_type_in_timespan_and_location(df, sys.argv[1], first_datetime, second_datetime,
                                                                       sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7])

    # Calculate the statistics for the filtered DataFrame
    calculated_statistics = calculate_statistics(filtered_df, STATISTIC_CRITERIA)

    # Print the statistics
    print_statistics(calculated_statistics)
    print_step("Printing vehicles above threshold")
    print("Number of vehicles above threshold :" + str(count_vehicles_above_threshold(filtered_df, "speed_kmh", 50.00)))
    print_vehicles_above_threshold(df, "speed_kmh", 80.00)
    # Stop the Spark session
    spark.stop()
