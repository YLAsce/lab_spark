package clusterdata

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, FileVisitOption}

object Task4SchedulingClassEviction {
    def execute() = {
        // Initialize schemas
        val schema_task_events = ReadSchema.read("task_events")
        schema_task_events.printTreeString()

        // Initialize spark session
        val sk = SparkSession.builder()
                              .appName("SCE")
                              .master("local[*]")
                              .getOrCreate()

        // Set level of log to ERROR
        sk.sparkContext.setLogLevel("ERROR")

        val df_task_events_var : DataFrame = sk.read
                                  .format("csv")
                                  .option("header", "false")
                                  .schema(schema_task_events)
                                  .load("./data/task_events/*.csv")

        val df_class_eviction = df_task_events_var.groupBy("job ID", "task index")
            .agg(
                max(when(col("event type").equalTo(2), 1).otherwise(0)).as("evicted"),
                first("scheduling class").as("scheduling class"),
            )

        val df_percent_eviction_by_class = df_class_eviction.groupBy("scheduling class")
            .agg(
                (sum(col("evicted")) / count("*")).as("percent_evicted")
            ).sort("scheduling class")

        df_percent_eviction_by_class.show()

        sk.stop()
    }
}