package parallelism

import zio.{ExitCode, URIO, ZEnv, ZIO}
import zio.buildFromAny
import zio.console.putStrLn

object Parallelism extends zio.App {

  override def run(args: List[String]): URIO[ZEnv, ExitCode] = {

    // foreachParN
    //mergeAllParN
    //reduceAllParN

    val a: List[ZIO[Any, Throwable, String]] = List(ZIO.succeed("success1"), ZIO.succeed("success2"))
//    val b: List[ZIO[Any, Throwable, String]] = List(ZIO("success1"), ZIO(new Exception("error1")))
//
//    val c = ZIO.collectParN(2)(a)(identity)
//      .flatMap(
//        collection => {
//          println(collection)
//          ZIO.succeed(collection)
//        }
//      ).as(ExitCode(0))

    val d = ZIO.foreachParN(2)(
      List("a", "b", "c", "d")
    )(a => putStrLn(s"$a").as(a)).unit


    //d
    ZIO.succeed(ExitCode(0))
  }
}
