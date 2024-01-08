package clusterdata

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, FileVisitOption}

object Task5SameJobMachine {
    def execute(onCloud: Boolean) = {
        // Initialize schemas
        val schema_task_events = ReadSchema.read("task_events")

        // Initialize spark session
        var skb = SparkSession.builder().appName("SJM")
        if(!onCloud) {
            println("Not run on cloud")
            skb = skb.master("local[*]")
        }
        val sk = skb.getOrCreate()

        // Set level of log to ERROR
        sk.sparkContext.setLogLevel("ERROR")

        var df_task_events_var : DataFrame = sk.read
                                  .format("csv")
                                  .option("header", "false")
                                  .schema(schema_task_events)
                                  .load("./data/task_events/*.csv")
        if(onCloud) {
            df_task_events_var = sk.read
                                  .format("csv")
                                  .option("header", "false")
                                  .option("compression", "gzip")
                                  .schema(schema_task_events)
                                  .load("gs://clusterdata-2011-2/task_events/*.csv.gz")
        }

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