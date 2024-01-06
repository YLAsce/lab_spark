package clusterdata

import scala.io.Source
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object ReadSchema {
    def read(name: String): StructType = {
        
        // 读取 schema CSV 文件
        val schemaFile = "./data/schema.csv"
        val schemaLines = Source.fromFile(schemaFile).getLines().toList

        // 选择 job_events 开头的行
        val jobEventsSchemaLines = schemaLines.filter(line => line.startsWith(name))

        // 创建 StructField 数组
        val fields = jobEventsSchemaLines.map { line =>
        val Array(_, _, name, dataType, mandatory) = line.split(",").map(_.trim)
        val sparkDataType = dataType match {
            case "INTEGER" => LongType
            case "STRING_HASH" => StringType
            case "FLOAT" => FloatType
            case "BOOLEAN" => BooleanType
            case "STRING_HASH_OR_INTEGER" => StringType
            // 添加其他可能的数据类型
        }
        StructField(name, sparkDataType, !mandatory.equalsIgnoreCase("YES"))
        }

        StructType(fields)

    }
}