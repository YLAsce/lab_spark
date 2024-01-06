package clusterdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.commons.math3.ml.clustering.Cluster

object Task1DistributionOfCPU {
   def execute() = {
      // start spark with 1 worker thread
      val conf = new SparkConf().setAppName("ClusterData").setMaster("local[1]")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

      // read the input file into an RDD[String]
      val machine_events_rdd = sc.textFile("./data/machine_events/part-00000-of-00001.csv").map(x => x.split(","))

      val rdd_CPU_Capacity = machine_events_rdd.map(x => (  
         try {
            // Extract the column of CPUs (5) and machine ID (2)
            // Notice that the index begins with 0 !
            val CPU_Capacity = x(4)
            val machine_ID = x(1)
            
            // Return the key-value pair 
            (CPU_Capacity, machine_ID)
         } catch {
            case e: ArrayIndexOutOfBoundsException =>
               // The row which exists "NA"
               println(s"ArrayIndexOutOfBoundsException on line: ${x.mkString(",")}")

               // Return the default pair
               ("NA", "NA")
         })
      ).groupByKey()

      val rdd_count = rdd_CPU_Capacity.map(x => (x._1, x._2.size))

      println("Distribution of the machines according to their CPU capacity: ")
      rdd_count.foreach(x => {
         val key = x._1
         val values = x._2
         println(s"CPU Capacity: $key, Number of the machines: $values")
      })

      sc.stop()
   }
}