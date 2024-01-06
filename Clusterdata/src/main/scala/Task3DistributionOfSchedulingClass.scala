package clusterdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.commons.math3.ml.clustering.Cluster
import java.nio.file.{Files, Paths, FileVisitOption}

object Task3DistributionOfSchedulingClass {
   def execute() = {
      // start spark with 1 worker thread
      val conf = new SparkConf().setAppName("ClusterData").setMaster("local[1]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

      // read the input file into an RDD[String]
      val files_job_events = Files.walk(Paths.get("./data/job_events"), FileVisitOption.FOLLOW_LINKS).toArray.map(x => x.toString())

      val files_task_events = Files.walk(Paths.get("./data/task_events"), FileVisitOption.FOLLOW_LINKS).toArray.map(x => x.toString())

      var total_job_events_rdd_var : RDD[Array[String]] = sc.parallelize(Seq())

      var total_task_events_rdd_var : RDD[Array[String]] = sc.parallelize(Seq())
      
      // Start from index 1, since index 0 stands for the path name of the directory
      for (i <- 1 to (files_job_events.size - 1)) {
        val one_file_rdd = sc.textFile(files_job_events(i)).map(x => x.split(","))        
        total_job_events_rdd_var = total_job_events_rdd_var.union(one_file_rdd)
      }

      for (i <- 1 to (files_task_events.size - 1)) {
         val one_file_rdd = sc.textFile(files_task_events(i)).map(x => x.split(","))        
         total_task_events_rdd_var = total_task_events_rdd_var.union(one_file_rdd)
      }

      // println("DEBUG: " + total_rdd_var.count())

      // println("DEBUG: " + total_rdd_var.count())

      total_job_events_rdd_var = total_job_events_rdd_var.filter(x => x(5) != "")

      val total_job_events_rdd = total_job_events_rdd_var

      total_task_events_rdd_var = total_task_events_rdd_var.filter(x => x(7) != "")

      // Set the key as (job ID, task index) to stand for one task
      // And set the value with (Scheduling class) only
      // At last, drop the duplicate rows
      val total_task_events_rdd = total_task_events_rdd_var.map(x => ((x(2), x(3)), x(7))).distinct()

      println("Distribution of Jobs: ")
      total_job_events_rdd.map(x => (x(5), 1)).reduceByKey((x, y) => (x + y)).foreach(
         x => {
            println(s"- Scheduling type: ${x._1}, Number of jobs: ${x._2}")
         }
      )

      println("Distribution of Tasks: ")
      total_task_events_rdd.map(x => (x._2, 1)).reduceByKey((x, y) => (x + y)).foreach(
         x => {
            println(s"- Scheduling type: ${x._1}, Number of tasks: ${x._2}")
         }
      )

      sc.stop()
   }
}