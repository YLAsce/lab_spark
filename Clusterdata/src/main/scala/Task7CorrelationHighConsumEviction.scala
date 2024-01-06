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

        var files_task_events = Files.walk(Paths.get("./data/task_events"), FileVisitOption.FOLLOW_LINKS).toArray
        var files_task_usage = Files.walk(Paths.get("./data/task_usage"), FileVisitOption.FOLLOW_LINKS).toArray

        var df_task_events_var : DataFrame = null
        var df_task_usage_var : DataFrame = null

        for (file <- files_task_events) {
          val filePath = file.toString()
          val df : DataFrame = sk.read
                                  .format("csv")
                                  .option("header", "false")
                                  .option("delimiter", ",")
                                  .schema(schema_task_events)
                                  .load(filePath)
          df.show()
          if (df_task_events_var == null) 
            df_task_events_var = df
          else 
            df_task_events_var = df_task_events_var.union(df)  
        }

        for (file <- files_task_usage) {
          val filePath = file.toString()
          val df : DataFrame = sk.read
                                  .format("csv")
                                  .option("header", "false")
                                  .option("delimiter", ",")
                                  .schema(schema_task_usage)
                                  .load(filePath)
          if (df_task_usage_var == null) 
            df_task_usage_var = df
          else 
            df_task_usage_var = df_task_usage_var.union(df)  
        }

        val taskEventsDF = df_task_events_var
        val taskUsageDF = df_task_usage_var

        // Task events: Col 6 -> Event type
        val renamedTaskEventsDF = taskEventsDF.withColumnRenamed("_c5", "Event type")

        // Task usage: Col 6 -> CPU rate (usage)
        val renamedTaskUsageDF = taskUsageDF.withColumnRenamed("_c5", "CPU rate")

        // Task events: Col 3 -> job ID, Col 4 -> task index, Col 5 -> machine ID
        // Task usage: Col 3 -> job ID, Col 4 -> task index, Col 5 -> machine ID
        val combinedDF : DataFrame = renamedTaskEventsDF.join(renamedTaskUsageDF, Seq("_c2", "_c3", "_c4"))

        val avgCPURate : Double = combinedDF.select("CPU rate").agg(avg("CPU rate")).collect()(0)(0).asInstanceOf[Double]

        val evictTask : DataFrame = combinedDF.select("CPU rate").filter(col("Event type") === 2)

        val evictTaskHighConsum : DataFrame = evictTask.filter(col("CPU rate") > avgCPURate)

        println("Total: " + combinedDF.count() + " Evict:" + evictTask.count() + " High Consum Evict: " + evictTaskHighConsum.count())

        // Shut down SparkSession
        sk.stop()
  }
}