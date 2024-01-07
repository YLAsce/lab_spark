package clusterdata

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, FileVisitOption}

object Task7CorrelationHighConsumEviction {
    def execute() = {
        // Initialize schemas
        val schema_task_events = ReadSchema.read("task_events")
        schema_task_events.printTreeString()
        val schema_task_usage = ReadSchema.read("task_usage")
        schema_task_usage.printTreeString()

        // Initialize SparkSession
        val sk = SparkSession.builder()
                              .appName("CHCE")
                              .master("local[*]")
                              .getOrCreate()

        // Set level of log to ERROR
        sk.sparkContext.setLogLevel("ERROR")

        // Read the data files (*.csv)
        val taskEventsDF = sk.read
                              .format("csv")
                              .option("header", "false")
                              .option("delimiter", ",")
                              .schema(schema_task_events)
                              .load("./data/task_events/*.csv")
        val taskUsageDF = sk.read
                            .format("csv")
                            .option("header", "false")
                            .option("delimiter", ",")
                            .schema(schema_task_usage)
                            .load("./data/task_usage/*.csv")

        // Extract the dataframe of the evicted tasks
        val evictedTasksDF = taskEventsDF.filter(col("Event type") === 2).select("job ID", "task index", "machine ID")
        // Extract the dataframe of the machines by the descending order of CPU consumption
        val taskUsageOrderedDF = taskUsageDF.select("machine ID", "CPU rate").orderBy(col("CPU rate").desc).distinct()
        
        // Define the threshold of "High consumption"
        val highConsumRate = 0.0001
        // Number of machines
        val numMachines = taskUsageOrderedDF.count()

        // Number of high-consuming machines
        val top : Int = (numMachines * highConsumRate).toInt
        // Extract the dataframe of high-consuming machines
        val topConsumingMachines = taskUsageOrderedDF.limit(top)

        // Combine the dataframes, extract the dataframe of high-consuming and evicted tasks
        val combinedEvictHighComsumDF = topConsumingMachines.join(evictedTasksDF, "machine ID")
                                                          .select("job ID", "task index", "machine ID")
                                                          .distinct()

        // Print logs
        println("Total High comusing machines: " + top + ", ")
        println("Total machines: " + numMachines + ", ")
        println("Total Evicted Tasks: " + evictedTasksDF.count() + ", ")
        println("Evicted Task on High Consuming machines: " + combinedEvictHighComsumDF.count())

        // Shut down SparkSession
        sk.stop()
  }
}