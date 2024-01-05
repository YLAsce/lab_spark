package Clusterdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.commons.math3.ml.clustering.Cluster

object PercentageOfCompPowerLost {
   def main(args: Array[String]) = {
      // start spark with 1 worker thread
      val conf = new SparkConf().setAppName("ClusterData").setMaster("local[1]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

      // read the input file into an RDD[String]
      val machine_events_rdd = sc.textFile("./src/main/data/machine_events/part-00000-of-00001.csv").map(x => x.split(","))

      val rdd_EventType_CPUCapacity = machine_events_rdd.map(x => (  
         try {
            // Extract the column of event type (3) and CPUs (5)
            // Notice that the index begins with 0 !
            val Event_Type = x(2)
            val CPU_Capacity = x(4)
            
            // Return the key-value pair 
            (Event_Type, CPU_Capacity)
         } catch {
            case e: ArrayIndexOutOfBoundsException =>
               // The row which exists "NA"
               println(s"ArrayIndexOutOfBoundsException on line: ${x.mkString(",")}")

               // Return the default pair
               ("NA", "NA")
         })
      )

      val rdd_EventType_CPUCapacity_filtered = rdd_EventType_CPUCapacity.filter(x => x._1 != "NA" && x._2 != "NA")

      val rdd_EventType_CPUCapacity_Convert = rdd_EventType_CPUCapacity_filtered.mapValues(value => value.toFloat)

      val rdd_EventType_CPUCapacity_SUM = rdd_EventType_CPUCapacity_Convert.reduceByKey((x, y) => (x + y))

      val total_sum = rdd_EventType_CPUCapacity_SUM.map(x => x._2).reduce(_ + _)
      val eventtype1_sum = rdd_EventType_CPUCapacity_SUM.filter(x => x._1 == "1").first()._2
      println("total sum: " + total_sum + "event type 1 sum: " + eventtype1_sum)
      println("Percentage of Lost Computational Power: " + ((eventtype1_sum / total_sum) * 100.0) + "%")

    //   val rdd_count = rdd_CPU_Capacity_SUM.map(x => (x._1, x._2))

      
    //   println("DEBUG: " + rdd_CPU_Capacity_SUM_filtered.first()._1 + ", " + rdd_CPU_Capacity_SUM.first()._2)
      rdd_EventType_CPUCapacity_SUM.foreach(x => {
         val key = x._1
         val values = x._2
         println(s"Event type: $key, Sum of CPU Capacity: $values")
      })

      sc.stop()
   }
}