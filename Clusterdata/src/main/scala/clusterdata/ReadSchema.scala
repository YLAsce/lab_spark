package clusterdata

import scala.io.Source
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

// Automatically generate Spark schema according to the schema.csv file
object ReadSchema {
    def read(name: String): StructType = {
        // Copy the schema.csv file here
        // Avoid building Scala file reading process on Google Cloud
        val schema_csv = 
"""
file pattern,field number,content,format,mandatory
job_events/part-?????-of-?????.csv.gz,1,time,INTEGER,YES
job_events/part-?????-of-?????.csv.gz,2,missing info,INTEGER,NO
job_events/part-?????-of-?????.csv.gz,3,job ID,INTEGER,YES
job_events/part-?????-of-?????.csv.gz,4,event type,INTEGER,YES
job_events/part-?????-of-?????.csv.gz,5,user,STRING_HASH,NO
job_events/part-?????-of-?????.csv.gz,6,scheduling class,INTEGER,NO
job_events/part-?????-of-?????.csv.gz,7,job name,STRING_HASH,NO
job_events/part-?????-of-?????.csv.gz,8,logical job name,STRING_HASH,NO
task_events/part-?????-of-?????.csv.gz,1,time,INTEGER,YES
task_events/part-?????-of-?????.csv.gz,2,missing info,INTEGER,NO
task_events/part-?????-of-?????.csv.gz,3,job ID,INTEGER,YES
task_events/part-?????-of-?????.csv.gz,4,task index,INTEGER,YES
task_events/part-?????-of-?????.csv.gz,5,machine ID,INTEGER,NO
task_events/part-?????-of-?????.csv.gz,6,event type,INTEGER,YES
task_events/part-?????-of-?????.csv.gz,7,user,STRING_HASH,NO
task_events/part-?????-of-?????.csv.gz,8,scheduling class,INTEGER,NO
task_events/part-?????-of-?????.csv.gz,9,priority,INTEGER,YES
task_events/part-?????-of-?????.csv.gz,10,CPU request,FLOAT,NO
task_events/part-?????-of-?????.csv.gz,11,memory request,FLOAT,NO
task_events/part-?????-of-?????.csv.gz,12,disk space request,FLOAT,NO
task_events/part-?????-of-?????.csv.gz,13,different machines restriction,BOOLEAN,NO
machine_events/part-00000-of-00001.csv.gz,1,time,INTEGER,YES
machine_events/part-00000-of-00001.csv.gz,2,machine ID,INTEGER,YES
machine_events/part-00000-of-00001.csv.gz,3,event type,INTEGER,YES
machine_events/part-00000-of-00001.csv.gz,4,platform ID,STRING_HASH,NO
machine_events/part-00000-of-00001.csv.gz,5,CPUs,FLOAT,NO
machine_events/part-00000-of-00001.csv.gz,6,Memory,FLOAT,NO
machine_attributes/part-00000-of-00001.csv.gz,1,time,INTEGER,YES
machine_attributes/part-00000-of-00001.csv.gz,2,machine ID,INTEGER,YES
machine_attributes/part-00000-of-00001.csv.gz,3,attribute name,STRING_HASH,YES
machine_attributes/part-00000-of-00001.csv.gz,4,attribute value,STRING_HASH_OR_INTEGER,NO
machine_attributes/part-00000-of-00001.csv.gz,5,attribute deleted,BOOLEAN,YES
task_constraints/part-?????-of-?????.csv.gz,1,time,INTEGER,YES
task_constraints/part-?????-of-?????.csv.gz,2,job ID,INTEGER,YES
task_constraints/part-?????-of-?????.csv.gz,3,task index,INTEGER,YES
task_constraints/part-?????-of-?????.csv.gz,4,comparison operator,INTEGER,YES
task_constraints/part-?????-of-?????.csv.gz,5,attribute name,STRING_HASH,YES
task_constraints/part-?????-of-?????.csv.gz,6,attribute value,STRING_HASH_OR_INTEGER,NO
task_usage/part-?????-of-?????.csv.gz,1,start time,INTEGER,YES
task_usage/part-?????-of-?????.csv.gz,2,end time,INTEGER,YES
task_usage/part-?????-of-?????.csv.gz,3,job ID,INTEGER,YES
task_usage/part-?????-of-?????.csv.gz,4,task index,INTEGER,YES
task_usage/part-?????-of-?????.csv.gz,5,machine ID,INTEGER,YES
task_usage/part-?????-of-?????.csv.gz,6,CPU rate,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,7,canonical memory usage,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,8,assigned memory usage,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,9,unmapped page cache,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,10,total page cache,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,11,maximum memory usage,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,12,disk I/O time,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,13,local disk space usage,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,14,maximum CPU rate,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,15,maximum disk IO time,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,16,cycles per instruction,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,17,memory accesses per instruction,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,18,sample portion,FLOAT,NO
task_usage/part-?????-of-?????.csv.gz,19,aggregation type,BOOLEAN,NO
task_usage/part-?????-of-?????.csv.gz,20,sampled CPU usage,FLOAT,NO
"""
        val schemaLines = schema_csv.split("\n").toList

        // Filter lines in the required file
        val jobEventsSchemaLines = schemaLines.filter(line => line.startsWith(name))

        // Build StructField array as Spark schema
        val fields = jobEventsSchemaLines.map { line =>
        val Array(_, _, name, dataType, mandatory) = line.split(",").map(_.trim)
        val sparkDataType = dataType match {
            case "INTEGER" => LongType
            case "STRING_HASH" => StringType
            case "FLOAT" => FloatType
            case "BOOLEAN" => BooleanType
            case "STRING_HASH_OR_INTEGER" => StringType
        }
        StructField(name, sparkDataType, !mandatory.equalsIgnoreCase("YES"))
        }

        // Return a StructType
        StructType(fields)

    }
}