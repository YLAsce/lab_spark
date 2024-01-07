package clusterdata

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, FileVisitOption}

object Task9CorrFlowTimeAndConsum {
    def execute() {
        // Initialize schemas
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
        val taskUsageDF = sk.read
                    .format("csv")
                    .option("header", "false")
                    .option("delimiter", ",")
                    .schema(schema_task_usage)
                    .load("./data/task_usage/*.csv")
        
        val taskUsageTimeDF = taskUsageDF.withColumn("Flow time", col("end time") - col("start time"))

        val taskUsageTimeSumDF = taskUsageTimeDF.select("job ID", "task index", "CPU rate", "canonical memory usage", "Flow time")
                                                .groupBy("job ID", "task index")
                                                .agg(sum("CPU rate").alias("SUM CPU Usage"),
                                                    sum("canonical memory usage").alias("SUM Mem Usage"),
                                                    sum("Flow time").alias("Sum Time consum"))

        val corrTimeCPU = taskUsageTimeSumDF.stat.corr("Sum Time consum", "SUM CPU Usage")

        val corrTimeMem = taskUsageTimeSumDF.stat.corr("Sum Time consum", "SUM Mem Usage")

        println("Correlation between Flow time and CPU usage: " + corrTimeCPU)
        println("Correlation between Flow time and Memory usage: " + corrTimeMem)

        // Shut down SparkSession
        sk.stop()
    }
}