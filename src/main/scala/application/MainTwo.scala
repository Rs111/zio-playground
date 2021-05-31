package application

import zio.console.{Console, putStrLn}
import zio.{Cause, ExitCode, Managed, Queue, Ref, Schedule, UIO, URIO, ZEnv, ZIO, ZLayer}
import zio.duration._
import zio.buildFromAny // required for forkAll

object MainEnvTwo {
  trait Service {
    val allTopics: UIO[Queue[String]]
    val topicsBeingConsumed: UIO[Ref[Set[String]]]
    val topicsToStartConsuming: UIO[Queue[String]]
  }

  val live: ZLayer[Any, Nothing, MainEnvTwo] = ZLayer.succeed(
    new Service {
      override val allTopics: UIO[Queue[String]] = Queue.unbounded[String]
      override val topicsBeingConsumed: UIO[Ref[Set[String]]] = Ref.make(Set.empty[String])
      override val topicsToStartConsuming: UIO[Queue[String]] = Queue.unbounded[String]
    }
  )
}

object MainTwo extends zio.App {

  def taskAddTopicsToQueue(queue: Queue[String], runNumber: Ref[Int]): ZIO[ZEnv, Throwable, Unit] = {
    for {
      _ <- runNumber.update(x => x + 1)
      x1 <- runNumber.get
      x <- if (x1.toInt == 5) ZIO.fail(new Exception("AAAA Run 5")) else ZIO.succeed(x1)
      topics =
      if (x <= 1)
        Set("a", "b")
      else if (Seq(2, 3).contains(x))
        Set("a", "b", "c")
      else
        Set("b", "c")
      _ <- putStrLn(s"Run $x, adding the following Kafka Topics to Queue --> ${topics.mkString(", ")}")
      _ <- queue.offerAll(topics)
    } yield ()
  }

  def queueIsEmpty(queue: Queue[String]): ZIO[Console, Nothing, Boolean] = {
    queue.size.flatMap(i =>
      putStrLn(s"Queue size is $i").as(i == 0)
    )
  }

  def updateTopicsToConsume(allTopicsQ: Queue[String], topicsBeingConsumed: Ref[Set[String]]): ZIO[ZEnv, Throwable, Queue[String]] = {
    for {
      allTopics <- allTopicsQ.takeAll
      _ <- if (allTopics.nonEmpty)
        putStrLn(s"Taking topics from Queue: ${allTopics.mkString(", ")}")
      else
        ZIO.succeed(())
      runningJobs <- topicsBeingConsumed.get
      topicsToConsume = allTopics.toSet.diff(runningJobs)
      topicsToConsumeQueue <- Queue.unbounded[String]
      _ <- topicsToConsumeQueue.offerAll(topicsToConsume)
      _ <- if (topicsToConsume.nonEmpty)
        putStrLn(s"Starting consumers for topics: ${topicsToConsume.mkString(", ")}")
      else
        ZIO.succeed(())
    } yield topicsToConsumeQueue
  }

  def createWorker(topicsToConsume: Queue[String], runningJobs: Ref[Set[String]]): ZIO[ZEnv, Throwable, Unit] = {
    for {
      topic <- topicsToConsume.take
      _ <- putStrLn(s"Starting Spark Job for topic $topic")
      _ <- runningJobs.update(set => set + topic)
      _ <- zio.random.nextIntBetween(1, 10).flatMap(i => ZIO.sleep(i.second)) //das wurken
      _ <- runningJobs.update(set => set - topic)
      _ <- putStrLn(s"Finishing Spark Job for topic $topic")
    } yield ()
  }

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    (
      for {
        env <- ZIO.environment[MainEnvTwo].provideLayer(MainEnvTwo.live)
        allTopics <- env.get.allTopics
        topicsBeingConsumed <- env.get.topicsBeingConsumed
        topicsToStartConsuming <- env.get.topicsToStartConsuming
        topicsAddJobNumber <- Ref.make(0)
        topicsQueryFiber <- queueIsEmpty(allTopics).flatMap(queueIsEmpty =>
          if (queueIsEmpty)
            taskAddTopicsToQueue(allTopics, topicsAddJobNumber)
          else
            ZIO.succeed(())
        )
          .repeat(Schedule.spaced(3.seconds)).fork
//        topicsToConsumeFiber <- getTopicsToConsume(allTopics, topicsBeingConsumed).repeat(Schedule.spaced(5.second)).fork
//        topicsConsumeFiber <-
//          (for {
//            topicsToConsume <- getTopicsToConsume(allTopics, topicsBeingConsumed)
//            workers         = List.fill(4)(createWorker(topicsToConsume, topicsBeingConsumed).forever)
//            fiber           <- ZIO.forkAll(workers)
//         //   join            <- fiber.join
//          } yield ()).repeat(Schedule.spaced(5.second)).fork
//        _ <- topicsQueryFiber.join
//        _ <- topicsConsumeFiber.join




//        topicsToConsume <- getTopicsToConsume(allTopics, topicsBeingConsumed)
//
//        topicsConsumeFiber <-
//          (for {
//            topicsToConsume <- getTopicsToConsume(allTopics, topicsBeingConsumed)
//            topicsToConsumeQueueSize <- topicsToConsume.size
//
//
//            workers = List.fill(4)(createWorker(topicsToConsume, topicsBeingConsumed))
//            fiber <- ZIO.forkAll(workers)
//          } yield ()).repeat(Schedule.spaced(5.second)).fork
//        topicsConsumeFiber <-
//          (for {
//            topicsToConsume <- getTopicsToConsume(queue, topicsBeingConsumed)
//            workers = List.fill(4)(createWorker(topicsToConsume, topicsBeingConsumed))
//            fiber <- ZIO.forkAll(workers)
//          } yield ()).repeat(Schedule.spaced(5.second)).fork
//        a <- topicsQueryFiber.await
//        b <- topicsConsumeFiber.await
//        exit <- ZIO.succeed(a.zip(b))
      } yield ()).foldCauseM(
      (cause: Cause[Throwable]) => putStrLn("NOOOOOOO") *> putStrLn(cause.prettyPrint).as(ExitCode(1)),
      _ => ZIO.succeed(ExitCode(0))
    )
  }
}
