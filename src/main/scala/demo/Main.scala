package demo

import zio.console.{Console, putStrLn}
import zio.{Cause, ExitCode, Fiber, Queue, Ref, Schedule, UIO, URIO, ZEnv, ZIO, ZLayer}
import zio.duration._

object MainEnv {
  trait Service {
    val mainQueue: UIO[Queue[String]]
    val runningJobs: UIO[Ref[Set[String]]]
  }

  val live: ZLayer[Any, Nothing, MainEnv] = ZLayer.succeed(
    new Service {
      override val mainQueue: UIO[Queue[String]] = Queue.unbounded[String]
      override val runningJobs: UIO[Ref[Set[String]]] = Ref.make(Set.empty[String])
    }
  )
}

object Main extends zio.App{

  def taskAddTopicsToQueue(queue: Queue[String], runNumber: Ref[Int]): ZIO[ZEnv, Throwable, Unit] = {
    for {
      _         <- runNumber.update(x => x + 1)
      x         <- runNumber.get
      topics    =
                  if(x <= 1)
                    Set("a", "b")
                  else if (Seq(2,3).contains(x))
                    Set("a", "b", "c")
                  else
                    Set("b", "c")
      _         <- putStrLn(s"Run $x, adding the following Kafka Topics to Queue --> ${topics.mkString(", ")}")
      _         <- queue.offerAll(topics)
    } yield ()
  }

  def taskAddTopicsToQueueFail(queue: Queue[String], runNumber: Ref[Int]): ZIO[ZEnv, Throwable, Unit] = {
    for {
      _         <- runNumber.update(x => x + 1)
      x1        <- runNumber.get
      x         <- if (x1.toInt == 5) ZIO.fail(new Exception("Radu Test Failure Run 5")) else ZIO.succeed(x1)
      topics    =
      if(x <= 1)
        Set("a", "b")
      else if (Seq(2,3).contains(x))
        Set("a", "b", "c")
      else
        Set("b", "c")
      _         <- putStrLn(s"Run $x, adding the following Kafka Topics to Queue --> ${topics.mkString(", ")}")
      _         <- queue.offerAll(topics)
    } yield ()
  }

  def queueIsEmpty(queue: Queue[String]): ZIO[Console, Nothing, Boolean] = {
    queue.size.flatMap(i =>
      putStrLn(s"Queue size is $i").as(i == 0)
    )
  }

  def createWorker(topic: String, runningJobs: Ref[Set[String]]): ZIO[ZEnv, Throwable, Unit] = {
    for {
      _ <- putStrLn(s"Starting Spark Job for topic $topic")
      _ <- runningJobs.update(set => set + topic)
      _ <- zio.random.nextIntBetween(1, 10).flatMap(i => ZIO.sleep(i.second)) //das wurken
      _ <- runningJobs.update(set => set - topic)
      _ <- putStrLn(s"Finishing Spark Job for topic $topic")
    } yield ()
  }

  def getTopicsToConsume(queue: Queue[String], topicsBeingConsumed: Ref[Set[String]]): ZIO[ZEnv, Throwable, Set[String]] = {
    for {
      allTopics       <- queue.takeAll
      _               <- if (allTopics.nonEmpty)
                           putStrLn(s"Taking topics from Queue: ${allTopics.mkString(", ")}")
                         else
                           ZIO.succeed(())
      runningJobs     <- topicsBeingConsumed.get
      topicsToConsume = allTopics.toSet.diff(runningJobs)
      _               <- if (topicsToConsume.nonEmpty)
                            putStrLn(s"Starting consumers for topics: ${topicsToConsume.mkString(", ")}")
                         else
                            ZIO.succeed(())
    } yield topicsToConsume
  }

  import zio.buildFromAny // required for forkAll

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    (
      for {
        // create Queue + running jobs Set
        env                 <- ZIO.environment[MainEnv].provideLayer(MainEnv.live)
        queue               <- env.get.mainQueue
        topicsBeingConsumed <- env.get.runningJobs
        // query all beacon topics from Kafka if topics queue is empty
        topicsAddJobNumber  <- Ref.make(0)
        topicsQueryFiber    <- queueIsEmpty(queue).flatMap(queueIsEmpty =>
                                  if (queueIsEmpty)
                                    taskAddTopicsToQueue(queue, topicsAddJobNumber)
                                  else
                                    ZIO.succeed(())
                               ).repeat(Schedule.spaced(3.seconds)).fork

        topicsConsumeFiber  <- (for {
                                  // find topics to consume
                                  topicsToConsume <- getTopicsToConsume(queue, topicsBeingConsumed)
                                  // for each topic to consume, create spark job
                                  workers = topicsToConsume.map(topic => createWorker(topic, topicsBeingConsumed)) //TODO need a way to limit parallelism
                                  // create logical fiber to manage all workers
                                  fiber <- ZIO.forkAll(workers.toList)
                                } yield ()).repeat(Schedule.spaced(5.second)).fork
        _ <- Fiber.joinAll(List(topicsQueryFiber, topicsConsumeFiber))
      } yield ()).foldCauseM(
        (cause: Cause[Throwable]) => putStrLn("NOOOOOOOO") *> putStrLn(cause.prettyPrint).as(ExitCode(1)),
        _ => ZIO.succeed(ExitCode(0))
      )
  }

}
