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
      case "task4" => Task4SchedulingClassEviction.execute()
      case "task5" => Task5SameJobMachine.execute()
      case "task6" => Task6ResourceRequestConsume.execute()
      case "task7" => Task7CorrelationHighConsumEviction.execute()
      case "task8" => Task8TaskConsumCPUAndMem.execute()
      case "task9" => Task9CorrFlowTimeAndConsum.execute()
      // 添加更多任务...
      case _ =>
        println(s"[ERROR] Unknown task ID: $taskName")
        System.exit(1)
    }
  }
}