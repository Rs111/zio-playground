package application

import application.model.Topic
import zio.Fiber.never.inheritRefs
import zio.{Cause, Exit, ExitCode, Has, IO, Managed, Queue, Ref, Schedule, Task, UIO, URIO, ZEnv, ZIO, ZLayer, ZManaged}
import zio.duration._

import java.io.InputStream
import zio.clock.Clock
import zio.console.{Console, getStrLn, putStrLn}
import zio.random.{Random, nextDouble}

import scala.io.Source

object test extends App {
  println("hu")
}

//trait Show {
//  def display(message: String): ZIO[Any, Nothing, Unit]
//}
//
//// implementation 1 Constructor
//val layer1: ZLayer[Any, Nothing, Has[Show]] = ZLayer.succeed( new Show {
//override def display(message: String): ZIO[Any, Nothing, Unit] =
//ZIO.effectTotal(println(message))
//}
//)

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

object Main extends zio.App {
  def readFile(s: String): Task[List[String]] = {
    ZIO.effect(getClass.getClassLoader.getResourceAsStream(s))
      .bracket(is => ZIO.succeed(is.close())) {
        is => ZIO.effect(Source.fromInputStream(is).getLines.toList.sorted)
      }
  }

  def getBeaconTopics: ZIO[ZEnv, Throwable, List[Topic]] = {
    ZIO.effect(List("a", "b", "c", "d").map(Topic))
  }

  def readAndPrint(s: String): ZIO[zio.console.Console, Throwable, List[String]] = {
    putStrLn("Starting") *> myAppLogic *> readFile(s)
      .flatMap(
        set => Task {
          set.foreach(println)
          set
        }
      )
  }

  val myAppLogic =
    for {
      _    <- putStrLn("Enter Kafka Topics")
      name <- getStrLn
      _    <- putStrLn(s"Hello, ${name}, welcome to ZIO!")
    } yield ()



  def taskAddTopicsToQueueOld(queue: Queue[String]): ZIO[ZEnv with MainEnv, Throwable, Unit] =
    for {
      env    <- ZIO.environment[MainEnv]
      stateStore  <- env.get.mainQueue
      _      <- putStrLn("Enter topics")
      topics <- getStrLn
      _      <- putStrLn(s"Kafka Topics: $topics")
      topicsSet = topics.split(",").toSet
      _ <- queue.offerAll(topicsSet)
    } yield ()


  def printQueueRecords(queue: Queue[String]): ZIO[ZEnv with MainEnv, Throwable, Unit] =
    for {
      stateStore    <- ZIO.environment[MainEnv]
      _      <- queue.takeAll.flatMap(l => putStrLn("Stuff in Queue --> " + l.mkString(",")))
    } yield ()



  // return type of run must be infallible [i.e. URIO]; need to handle errors to make it work
  def runOld(args: List[String]): URIO[ZEnv, ExitCode] = {
    (
      for {
        env    <- ZIO.environment[MainEnv].provideLayer(MainEnv.live)
        queue  <- env.get.mainQueue
        fiberTest <- (putStrLn(".") *> ZIO.sleep(1.second)).forever.fork

        fiber  <- taskAddTopicsToQueueOld(queue)
                    .provideLayer(ZEnv.live ++ MainEnv.live)
                    .repeat(Schedule.spaced(5.seconds) && Schedule.recurs(1))
                    .fork
        _      <- printQueueRecords(queue)
                    .provideLayer(ZEnv.live ++ MainEnv.live) *> fiber.interrupt *> fiberTest.interrupt
    } yield ExitCode(0))
      .catchAllCause(cause => putStrLn(cause.prettyPrint) *> ZIO.succeed(ExitCode(1)))
      //.orElse(ZIO.succeed(ExitCode(1)))
  }

  def taskAddTopicsToQueue(queue: Queue[String], runNumber: Ref[Int]): ZIO[ZEnv, Throwable, Unit] = {
    for {
      _         <- runNumber.update(x => x + 1)
      x1        <- runNumber.get
      x         <- if (x1.toInt == 5) ZIO.fail(new Exception("AAAA Run 5")) else ZIO.succeed(x1)
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

  final case class PiState(inside: Double, total: Double)
  // ZIO Ref is atomic
  def update(ref: Ref[PiState]): ZIO[Random, Nothing, Unit] = {
    val randomPoint: ZIO[Random, Nothing, (Double, Double)] = nextDouble zip nextDouble

    for {
      ref <- Ref.make(PiState(0L, 0L))
      tuple <- randomPoint
      (x, y) = tuple
      _ <- ref.update(state => PiState(state.inside + x, y))
    } yield ()
  }

  import zio.buildFromAny // required for forkAll
  def runWorkers(args: List[String]): ZIO[ZEnv, Nothing, ExitCode] = {
    (for {
      ref <- Ref.make(PiState(0L, 0L))
      worker = update(ref) // makes worker that does a single update
      workerF = update(ref).forever  // makes a worker that never stops
      workers = List.fill(4)(workerF) // create 4 workers; remember, ZIO effects are just regular values, so you can treat them as such
               // for every effect/worker inside of the list, fork each of the effect inside the list and then compose fibers together
                // composed fiber allows us to control all fibers at once
      fiber <- ZIO.forkAll(workers) // each worker is in it's own fiber, but we control them from this logical fiber
      fiber2 <- (putStrLn("1") *> ZIO.sleep(1.second)).forever.fork
      _      <- putStrLn("Ener any key to terminate...")
      _      <- getStrLn *> fiber.interrupt *> fiber2.interrupt
    } yield ExitCode(0)).orElse(ZIO.succeed(ExitCode(1)))
  }

  def queueIsEmpty(queue: Queue[String]): ZIO[Console, Nothing, Boolean] = {
    queue.size.flatMap(i =>
      putStrLn(s"Queue size is $i").as(i == 0)
    )
  }

  def mockSparkTask(topic: String): ZIO[ZEnv, Throwable, Unit] = {
    putStrLn(s"Spark processing topic: $topic") *> ZIO.sleep(5.second)
  }

//  def createWorker(topic: String, runningJobs: Ref[Set[String]]): ZIO[ZEnv, Throwable, Unit] = {
//    for {
//      runningJobs <- runningJobs.get
//      _           <- if (runningJobs.contains(topic))
//                        ZIO.succeed(())
//                     else
//
//
//    } yield ()
//  }

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

  def addJobsToRunToRef(jobsToRun: Set[String], runningJobs: Ref[Set[String]]): ZIO[ZEnv, Throwable, Unit] = {
    runningJobs.update(runningJobs => runningJobs ++ jobsToRun)
  }

  def runOldNewer(args: List[String]): URIO[ZEnv, ExitCode] = {
    (
      for {
        env                 <- ZIO.environment[MainEnv].provideLayer(MainEnv.live)
        queue               <- env.get.mainQueue
        topicsBeingConsumed <- env.get.runningJobs
        topicsAddJobNumber  <- Ref.make(0)
        topicsQueryFiber    <- queueIsEmpty(queue).flatMap(queueIsEmpty =>
                                 if (queueIsEmpty)
                                   taskAddTopicsToQueue(queue, topicsAddJobNumber)
                                 else
                                   ZIO.succeed(())
                                   //putStrLn("Queue is not empty").unit
                               )
                                //.repeat(Schedule.spaced(3.seconds) && Schedule.recurs(7)).fork
                                .repeat(Schedule.spaced(3.seconds)).fork
//                                    .bracket(
//                                    release => release.interrupt
//                                  )(use => ZIO.succeed(use))
        topicsConsumeFiber  <- (for {
                                  topicsToConsume <- getTopicsToConsume(queue, topicsBeingConsumed)
                                  workers = topicsToConsume.map(topic => createWorker(topic, topicsBeingConsumed))
                                  fiber <- ZIO.forkAll(workers.toList)
                               } yield ()).repeat(Schedule.spaced(5.second)).fork
        _                  <- putStrLn("Enter any key to terminate...")
        _                  <- getStrLn
      //  _                  <- queue.takeAll.flatMap(l => putStrLn("Stuff in Queue --> " + l.mkString(", ")))
      //  _                  <- topicsQueryFiber.interrupt *> topicsConsumeFiber.interrupt
      } yield ExitCode(0))
      .catchAllCause(cause => ZIO.succeed(cause.prettyPrint).as(ExitCode(1)))
     // .orElse(ZIO.succeed(ExitCode(1)))

    //.orElse(ZIO.succeed(ExitCode(1)))
  }






  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    (
      for {
        env                 <- ZIO.environment[MainEnv].provideLayer(MainEnv.live)
        queue               <- env.get.mainQueue
        topicsBeingConsumed <- env.get.runningJobs
        topicsAddJobNumber  <- Ref.make(0)
        topicsQueryFiber    <- queueIsEmpty(queue).flatMap(queueIsEmpty =>
                                  if (queueIsEmpty)
                                    taskAddTopicsToQueue(queue, topicsAddJobNumber)
                                  else
                                    ZIO.succeed(())
                                )
                                  .repeat(Schedule.spaced(3.seconds)).fork
//        topicsConsumeFiber  <-
//                                (for {
//                                  topicsToConsume <- getTopicsToConsume(queue, topicsBeingConsumed)
//                                  topicSubset <- ZIO.succeed(topicsToConsume.sliding(4))
//                                  topicSubset
////                                  _ <- for {
////                                    topicSubSet <- topicsToConsume.sliding(4)
////
////                                  } yield(ZIO.succeed(topicSubSet))
//                                  workers <- topicsToConsume
//                                            .sliding(4)
//                                            .map { topics =>
//
//                                              val tops = topics.map(topic => createWorker(topic, topicsBeingConsumed))
//                                              ZIO.forkAll(tops.toList)
//                                            }
//
//
//                                  //fiber <- ZIO.forkAll(workers.toList)
//                                } yield ()).repeat(Schedule.spaced(5.second)).fork
        topicsConsumeFiber  <-
                               (for {
                                  topicsToConsume <- getTopicsToConsume(queue, topicsBeingConsumed)
                                  workers = topicsToConsume.map(topic => createWorker(topic, topicsBeingConsumed))
                                  fiber <- ZIO.forkAll(workers.toList)
                                } yield ()).repeat(Schedule.spaced(5.second)).fork
       // _ <- topicsQueryFiber.join.zip(topicsConsumeFiber.join)
        _ <- topicsQueryFiber.join
        _ <- topicsConsumeFiber.join
       // _ <- topicsConsumeFiber.join
      //  b <- topicsConsumeFiber.join
      //  exit <- topicsQueryFiber.await.flatMap(IO.done(_)) <* inheritRefs

       // <- a.
//        b <- topicsConsumeFiber.await
//        exit <- ZIO.succeed(a.zip(b)).flatMap(IO.done(_)) <* inheritRefs
       // (topicsQueryFiberA, topicsConsumeFiberA) <- topicsQueryFiber.await zip topicsConsumeFiber.await

      } yield ()).as(ExitCode(0)).orElse(ZIO.succeed(ExitCode(1)))
//      .foldCauseM(
//        (cause: Cause[Throwable]) => putStrLn(cause.prettyPrint).as(ExitCode(1)),
//        _ => ZIO.succeed(ExitCode(0))
//      )

    //.orElse(ZIO.succeed(ExitCode(1))
    //  .catchAllCause(cause => ZIO.succeed(cause.prettyPrint).as(ExitCode(1)))
    // .orElse(ZIO.succeed(ExitCode(1)))

    //.orElse(ZIO.succeed(ExitCode(1)))
  }

}
