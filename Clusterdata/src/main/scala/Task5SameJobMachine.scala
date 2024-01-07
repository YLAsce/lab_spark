package clusterdata

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, FileVisitOption}

object Task5SameJobMachine {
    def execute() = {
        // Initialize schemas
        val schema_task_events = ReadSchema.read("task_events")

        // Initialize spark session
        val sk = SparkSession.builder()
                              .appName("SJM")
                              .master("local[*]")
                              .getOrCreate()

        // Set level of log to ERROR
        sk.sparkContext.setLogLevel("ERROR")

        val df_task_events_var : DataFrame = sk.read
                                  .format("csv")
                                  .option("header", "false")
                                  .schema(schema_task_events)
                                  .load("./data/task_events/*.csv")

        val df_job_distinct_machine_num = df_task_events_var.groupBy("job ID")
            .agg(
                countDistinct("machine ID").as("num machine used")
            )

        df_job_distinct_machine_num.cache()

        df_job_distinct_machine_num.groupBy("num machine used")
            .count().sort("num machine used").show()

        println("Number of Jobs in which tasks use 1 machine:")
        println(df_job_distinct_machine_num.filter(col("num machine used") === 1).count())
        println("Number of Jobs:")
        println(df_job_distinct_machine_num.count())
        
        sk.stop()
    }
}