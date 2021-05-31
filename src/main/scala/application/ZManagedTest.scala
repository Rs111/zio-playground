package application

object ZManagedTest extends zio.App {

  import zio._
  import zio.console._
  import zio.duration.Duration

  import scala.concurrent.duration._

  val repeatable = putStrLn("ttl") repeat Schedule.fixed(Duration.fromScala(1.second)).unit
  val managed = ZManaged.make(repeatable.fork)(_.interrupt)
  val io = managed.use(_ => putStrLn("Use"))

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {
    //io.as(ExitCode(0))
    repeatable.toManaged_.fork.use(_ => putStrLn("Use").as(ExitCode(0)))
  }
}
