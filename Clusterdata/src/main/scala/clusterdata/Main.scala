package clusterdata

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("[ERROR] Usage: spark-submit target/scala-2.12/clusterdata_2.12-1.0.jar [taskID]")
      System.exit(1)
    }

    val onCloud = if (args(1) == "cloud") true else false
    
    val taskName = args(0)
    // 根据 taskName 执行相应的任务逻辑
    taskName match {
      case "task1" => Task1DistributionOfCPU.execute(onCloud)
      case "task2" => Task2PercentageOfCompPowerLost.execute(onCloud)
      case "task3" => Task3DistributionOfSchedulingClass.execute(onCloud)
      case "task4" => Task4SchedulingClassEviction.execute(onCloud)
      case "task5" => Task5SameJobMachine.execute(onCloud)
      case "task6" => Task6ResourceRequestConsume.execute(onCloud)
      case "task7" => Task7CorrelationHighConsumEviction.execute(onCloud)
      case "task8" => Task8TaskConsumCPUAndMem.execute(onCloud)
      case "task9" => Task9CorrFlowTimeAndConsum.execute(onCloud)
      // 添加更多任务...
      case _ =>
        println(s"[ERROR] Unknown task ID: $taskName")
        System.exit(1)
    }
  }
}