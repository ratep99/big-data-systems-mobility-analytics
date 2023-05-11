import pyspark.sql
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col

#Method used to get cleaned dataset, ready for analysis
'''
def cleanData():
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
    #vehicles_df = vehicles_df.select("timestamp", "id", "type", "latitude", "longitude",
                                 #    "speed_kmh", "acceleration", "distance", "odometer", "pos", "lane")

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


    #vehicles_df.coalesce(1).write.format("csv").option("header", "true").save("denverVehiclesCleaned.csv")
 '''
# Initialize Spark Application name
if __name__ == '__main__':
    #if len(sys.argv) < 2:
     #   print("Usage: main.py <input folder> ")
      #  exit(-1)
    appName = "DenverMobility"

    # Define Spark Configuration
    conf = SparkConf()
    conf.setMaster("spark://spark-master:7077")
    spark = SparkSession.builder.config(conf=conf).appName(appName).getOrCreate()

    # Set the log level to ERROR to reduce the amount of output
    spark.sparkContext.setLogLevel("ERROR")

    cleaned_df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)
    #cleaned_df.show()

    # Define the start and end timestamps
    start_time = "2023-02-11 09:00:00"
    end_time = "2023-02-11 10:00:00"

    start_latitude = 0
    start_longitude = 0

    end_latitude = 0
    end_longitude = 0

    # Filter the DataFrame to get the data for the car type vehicles between start_time and end_time
    df_filtered = cleaned_df.filter((col("type") == "car") & (col("timestamp").between(start_time, end_time)))


    df_filtered.show()
    print("Filtrirano")

    # Calculate the statistics
    statistics = df_filtered.agg(mean("speed_kmh"), max("speed_kmh"), min("speed_kmh"), stddev("speed_kmh")).collect()[0]

    # Print the statistics
    print("Mean speed: ", statistics[0])
    print("Max speed: ", statistics[1])
    print("Min speed: ", statistics[2])
    print("Standard deviation of speed: ", statistics[3])

    # Calculate the statistics for odometer
    odo_stats = df_filtered.agg(mean("odometer"), max("odometer"), min("odometer"), stddev("odometer")).collect()[0]

    # Print the statistics for odometer
    print("Mean odometer: ", odo_stats[0])
    print("Max odometer: ", odo_stats[1])
    print("Min odometer: ", odo_stats[2])
    print("Standard deviation of odometer: ", odo_stats[3])

    # Calculate the statistics for acceleration
    acc_stats = df_filtered.agg(mean("acceleration"), max("acceleration"), min("acceleration"), stddev("acceleration")).collect()[0]

    # Print the statistics for acceleration
    print("Mean acceleration: ", acc_stats[0])
    print("Max acceleration: ", acc_stats[1])
    print("Min acceleration: ", acc_stats[2])
    print("Standard deviation of acceleration: ", acc_stats[3])
    # Show the statistics in a table-like format
    #df_statistics.show()

    spark.stop()

