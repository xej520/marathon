package mesosphere.marathon
package upgrade

import mesosphere.marathon.core.condition.Condition
import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.state.{ KillSelection, Timestamp }

case class ScalingProposition(tasksToKill: Option[Seq[Instance]], tasksToStart: Option[Int])

//Proposition 建议，提议，主张
//ScalingProposition 扩容建议
object ScalingProposition {

  //定义一个函数propose
  //建议
  def propose(
    runningTasks: Seq[Instance],
    toKill: Option[Seq[Instance]],
    meetConstraints: ((Seq[Instance], Int) => Seq[Instance]),
    scaleTo: Int,//个数
    killSelection: KillSelection = KillSelection.DefaultKillSelection // killSelection设定了默认值
             ): ScalingProposition = {

    //计算一下，正在运行的task里，状态是killing的个数
    val killingTaskCount = runningTasks.count(_.state.condition == Condition.Killing)
    //打印出 正在运行的task数量，
    println("------<ScalingProposition.scala>----正在运行的task数量--->runningTasks.size\t" + runningTasks.size)
    println("-----打印出-----当前---运行的----task-----信息-------")
    runningTasks.foreach{
      x => println(x + " ")
    }
    //打印出 打算killing的task数量
    println("------<ScalingProposition.scala>------->killingTaskCount\t" + killingTaskCount)

    //类型转换，输入类型是Seq[Instance],转换成了Map[id, Instance]
    val runningTaskMap = Instance.instancesById(runningTasks)

    // 同样是类型转换，打算kill掉的task的转换
    val toKillMap = Instance.instancesById(toKill.getOrElse(Seq.empty))

    val (sentencedAndRunningMap, notSentencedAndRunningMap) = runningTaskMap partition {
      case (k, v) =>
        toKillMap.contains(k)
    }
    // overall number of tasks that need to be killed
    println("------<ScalingProposition.scala>------overall---num---task----runningTasks.size---" + runningTasks.size)
    println("------<ScalingProposition.scala>------overall---num---task----killingTaskCount----" + killingTaskCount)
    println("------<ScalingProposition.scala>------overall---num---task----scaleTo-------------" + scaleTo)
    println("------<ScalingProposition.scala>------overall---num---task----sentencedAndRunningMap.size------" + sentencedAndRunningMap.size)

    val killCount = math.max(runningTasks.size - killingTaskCount - scaleTo, sentencedAndRunningMap.size)
    println("------<ScalingProposition.scala>------overall---num---task----killCount------" + killCount)

    // tasks that should be killed to meet constraints – pass notSentenced & consider the sentenced 'already killed'
    val killToMeetConstraints = meetConstraints(
      notSentencedAndRunningMap.values.to[Seq],
      killCount - sentencedAndRunningMap.size
    )

    println("-------<ScalingProposition.scala>--------------xej------单独测试-------")
    println("-------<ScalingProposition.scala>--------------xej------遍历-----notSentencedAndRunningMap-------")
    notSentencedAndRunningMap.foreach(println)
    println("-------<ScalingProposition.scala>--------------xej------单独测试-------")

    // rest are tasks that are not sentenced and need not be killed to meet constraints
    val rest: Seq[Instance] = (notSentencedAndRunningMap -- killToMeetConstraints.map(_.instanceId)).values.to[Seq]

    //基本语法是，将Seq(A,B,C,D).flatten； A,B,C,D是4个集合
    //flatten就是将A，B,C,D里面的元素展开，重新组合成一个新的集合
    val ordered =
      Seq(sentencedAndRunningMap.values, killToMeetConstraints,
        rest.sortWith(sortByConditionAndDate(killSelection))).flatten

    //take 取出集合ordered里的前killCount个元素
    //这些instance，是打算要删除的；也就是候选的意思
    val candidatesToKill = ordered.take(killCount)

    println("------<ScalingProposition.scala>-----scaleTo----:\t" + scaleTo)
    println("------<ScalingProposition.scala>-----runningTasks.size----:\t" + runningTasks.size)
    println("------<ScalingProposition.scala>-----killCount----:\t" + killCount)
    //numberOfTasksToStart 表达的意思，应该是，从那个位置开始删除task
    val numberOfTasksToStart = scaleTo - runningTasks.size + killCount
    println("------<ScalingProposition.scala>-----numberOfTasksToStart----:\t" + numberOfTasksToStart)

    //candidatesToKill，判断打算要删除的task集合是否为空，不为空的话，说明，有的task需要删掉的
    val tasksToKill = if (candidatesToKill.nonEmpty) Some(candidatesToKill) else None
    val tasksToStart = if (numberOfTasksToStart > 0) Some(numberOfTasksToStart) else None

    println("-----------<ScalingProposition.scala>-----tasksToKill-----\t" + tasksToKill) //None
    println("-----------<ScalingProposition.scala>-----tasksToStart---\t" + tasksToStart)  //None， Some(1) 需要运行1个实例

    //真够笨的，下面是返回一个ScalingProposition对象啊
    ScalingProposition(tasksToKill, tasksToStart)
  }

  // TODO: this should evaluate a task's health as well
  /**
    * If we need to kill tasks, the order should be LOST - UNREACHABLE - UNHEALTHY - STAGING - (EVERYTHING ELSE)
    * If two task are staging kill with the latest staging timestamp.
    * If both are started kill the one according to the KillSelection.
    *
    * @param select Defines which instance to kill first if both have the same state.
    * @param a The instance that is compared to b
    * @param b The instance that is compared to a
    * @return true if a comes before b
    */
  def sortByConditionAndDate(select: KillSelection)(a: Instance, b: Instance): Boolean = {
    val weightA = weight(a.state.condition)
    val weightB = weight(b.state.condition)

    if (weightA < weightB) true
    else if (weightA > weightB) false
    else if (a.state.condition == Condition.Staging && b.state.condition == Condition.Staging) {
      // Both are staging.
      select(stagedAt(a), stagedAt(b))
    } else if (a.state.condition == Condition.Starting && b.state.condition == Condition.Starting) {
      select(a.state.since, b.state.since)
    } else {
      // Both are assumed to be started.
      // None is actually an error case :/
      (a.state.activeSince, b.state.activeSince) match {
        case (None, Some(_)) => true
        case (Some(_), None) => false
        case (Some(left), Some(right)) => select(left, right)
        case (None, None) => true
      }
    }

  }

  /** tasks with lower weight should be killed first */
  private val weight: Map[Condition, Int] = Map[Condition, Int](
    Condition.Unreachable -> 1,
    Condition.Staging -> 2,
    Condition.Starting -> 3,
    Condition.Running -> 4).withDefaultValue(5)

  private def stagedAt(instance: Instance): Timestamp = {
    val stagedTasks = instance.tasksMap.values.map(_.status.stagedAt)
    if (stagedTasks.nonEmpty) stagedTasks.max else Timestamp.now()
  }
}
