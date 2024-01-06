package clusterdata

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("[ERROR] Usage: spark-submit target/scala-2.12/clusterdata_2.12-1.0.jar [taskID]")
      System.exit(1)
    }

    val taskName = args(0)
    // 根据 taskName 执行相应的任务逻辑
    taskName match {
      case "task1" => Task1DistributionOfCPU.execute()
      case "task2" => Task2PercentageOfCompPowerLost.execute()
      case "task3" => Task3DistributionOfSchedulingClass.execute()
      case "task7" => Task7CorrelationHighConsumEviction.execute()
      // 添加更多任务...
      case _ =>
        println(s"Unknown task: $taskName")
        System.exit(1)
    }
  }
}