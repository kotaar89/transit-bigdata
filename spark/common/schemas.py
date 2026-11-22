from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField, StructType)

VEHICLE_POSITION = StructType([
    StructField("vehicle_id", StringType()),
    StructField("trip_id", StringType()),
    StructField("route_id", StringType()),
    StructField("direction_id", IntegerType()),
    StructField("start_date", StringType()),
    StructField("timestamp", LongType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("current_stop_sequence", IntegerType()),
    StructField("stop_id", StringType()),
    StructField("current_status", StringType()),
])

WEATHER = StructType([
    StructField("date", StringType()),
    StructField("hour", IntegerType()),
    StructField("precip_mm", DoubleType()),
    StructField("temp_c", DoubleType()),
    StructField("snow", IntegerType()),
    StructField("timestamp", LongType()),
])
