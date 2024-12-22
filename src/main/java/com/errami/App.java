package com.errami;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class App {
    public static void main(String[] args) throws Exception {
        // Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Hospital Incident Streaming")
                .getOrCreate();

        // Define the schema of the CSV file
        StructType schema = new StructType()
                .add("Id", DataTypes.IntegerType)
                .add("titre", DataTypes.StringType)
                .add("description", DataTypes.StringType)
                .add("service", DataTypes.StringType)
                .add("date", DataTypes.StringType);

        // Read streaming data from CSV files
        Dataset<Row> incidentStream = spark.readStream()
                .schema(schema)  // Specify the schema here
                .option("header", "true")
                .csv("hdfs://namenode:8020/incidents"); // Directory where new CSV files will be placed

        // Process 1: Display the number of incidents per service
        Dataset<Row> incidentsByService = incidentStream
                .groupBy("service")
                .count()
                .withColumnRenamed("count", "incident_count");

        // Write the result to console (continuous display of incidents per service)
        incidentsByService.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();

        // Process 2: Extract year from 'date' and calculate incidents per year
        Dataset<Row> incidentsByYear = incidentStream
                .withColumn("parsed_date", to_date(col("date"), "M/d/yyyy")) // parsing the date with the correct format
                .withColumn("year", year(col("parsed_date"))) // extracting year from the parsed date
                .groupBy("year")
                .count()
                .withColumnRenamed("count", "incident_count")
                .orderBy(col("incident_count").desc());

        // Write the result to console, showing the years with most incidents
        incidentsByYear.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();

        // Wait for the streams to finish (run indefinitely)
        spark.streams().awaitAnyTermination();
    }
}