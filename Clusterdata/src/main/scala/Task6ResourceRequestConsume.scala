package clusterdata

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, FileVisitOption}

object Task6ResourceRequestConsume {
    def execute() = {
        // Initialize schemas
        val schema_task_events = ReadSchema.read("task_events")
        schema_task_events.printTreeString()
        val schema_task_usage = ReadSchema.read("task_usage")
        schema_task_usage.printTreeString()

        // Initialize SparkSession
        val sk = SparkSession.builder()
                              .appName("RRC")
                              .master("local[*]")
                              .getOrCreate()

        // Set level of log to ERROR
        sk.sparkContext.setLogLevel("ERROR")

        // Read the data files (*.csv)
        val df_task_events = sk.read
                              .format("csv")
                              .option("header", "false")
                              .option("delimiter", ",")
                              .schema(schema_task_events)
                              .load("./data/task_events/*.csv")
        val df_task_usage = sk.read
                            .format("csv")
                            .option("header", "false")
                            .option("delimiter", ",")
                            .schema(schema_task_usage)
                            .load("./data/task_usage/*.csv")    

        val df_task_resource_request = df_task_events.groupBy("job ID", "task index")
            .agg(
                avg("CPU request").as("CPU request"),
                avg("memory request").as("memory request"),
                avg("disk space request").as("disk request")
            )

        val df_task_resource_usage = df_task_usage.groupBy("job ID", "task index")
            .agg(
                avg("CPU rate").as("CPU usage"),
                avg("assigned memory usage").as("memory usage"),
                avg("local disk space usage").as("disk usage")
            )

        val df_joined_request_usage = df_task_resource_request.join(df_task_resource_usage, Seq("job ID", "task index"))
        //df_joined_request_usage.show()

        df_joined_request_usage.cache()

        println("Correlation between CPU request and usage:")
        println(df_joined_request_usage.stat.corr("CPU request", "CPU usage"))

        println("Correlation between memory request and usage:")
        println(df_joined_request_usage.stat.corr("memory request", "memory usage"))

        println("Correlation between disk request and usage:")
        println(df_joined_request_usage.stat.corr("disk request", "disk usage"))
    }
}