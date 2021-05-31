package layers

import zio.{Has, ZIO, ZLayer}
import zio.Runtime.default.unsafeRun

object ZIOLayers extends App {
ZIO.apply()

 /* ZIO[R, E, A] = this program has dependency of type R, might fail with error e, might succeed with A
  - is a description of our program
  - can compose many of these ZIOs together to build sync/asyc/concurrent
  */

  // no requirements, never fails, returns Unit
  // returns a description; Zio is a description
  // for true result, we need zio.Runetime
  // can think of this as Any => Either[Nothing, Unit]
  def show(message: String): ZIO[Any, Nothing, Unit] = ZIO.effectTotal(println(message))

  unsafeRun(show("cheese"))

  // for ZIO, the R is contravariant; this means that for our show function, we can feed it subtype of Any
  // but how do we feed this function?
  // there is a function called provide layer that we can use to provide the R
  // layers in ZIO describe how input services are used to create output services

  // create service that provides us functionality to display a given message
  // show can have different implementations; Zlayer is a constructor that contains these implementations
  // NOTE: abstract services should have few dependencies; add the dependencies when making the concrete layers
  trait Show {
    def display(message: String): ZIO[Any, Nothing, Unit]
  }

  // implementation 1 Constructor
  val layer1: ZLayer[Any, Nothing, Has[Show]] = ZLayer.succeed( new Show {
    override def display(message: String): ZIO[Any, Nothing, Unit] =
      ZIO.effectTotal(println(message))
  }
  )

  // implementation 2; enables us to test whether message was displayed as expected
  // we can use a state here; either a mutable list of lines or zio.Ref
  case class ShowTest(lines: zio.Ref[List[String]]) extends Show {
    override def display(message: String): ZIO[Any, Nothing, Unit] =
      lines.update(_ :+ message)
  }

  // construct a layer using this implementation
  // we need to have an initial state
  def layer2(ref: zio.Ref[List[String]]): ZLayer[Any, Nothing, Has[Show]] =
    ZLayer.succeed(ShowTest(ref))


  // third implementation
  case class Config(messageAddOn: String, spaces: Int)
  case class ConfigTwo(messageAddOn2: String, spaces2: Int)

  val layer3: ZLayer[Has[Config], Nothing, Has[Show]] = {
    // create alias from input
    ZLayer.fromService(config => new Show {
      override def display(message: String): ZIO[Any, Nothing, Unit] =
        ZIO.effectTotal(println(message + config.messageAddOn))
    })
  }

  val layer32: ZLayer[Has[ConfigTwo] with Has[Config], Nothing, Has[Show]] = {
    // create alias from input
    ZLayer.fromServices[Config, ConfigTwo, Show]{ case (config, configTwo) => new Show {
      override def display(message: String): ZIO[Any, Nothing, Unit] =
        ZIO.effectTotal(println(message + config.messageAddOn))
    }}
  }


  // now lets use the show Service and make our program have a dependency on this Service
  // THREE ways of writing this
  def showTwo(message: String): ZIO[Has[Show], Nothing, Unit] = {
    ZIO.service[Show].flatMap(showService => showService.display(message))
    ZIO.accessM[Has[Show]](hasShow => hasShow.get.display(message))

    // using the companion object of Show
    Show.display(message)
  }

  // we can also create a companion object to show to access the display
  object Show {
    def display(message: String): ZIO[Has[Show], Nothing, Unit] =
      ZIO.accessM[Has[Show]](hasShow => hasShow.get.display(message))
  }

  def func[A]: Has[A] = ???

  // providing a layer; provide layer seems to work for any ZIO[Has[Service],...]
  val showUsingLayer1: ZIO[Any, Nothing, Unit] =
    showTwo("Hello World").provideLayer(layer1)

  val showUsingLayer2: ZIO[Any, Nothing, Boolean] =
    for {
      state <- zio.Ref.make(List.empty[String]) // make(l) => UIO(Ref(l))
      _     <- (showTwo("Hi!") *> showTwo("Bonjour!")).provideLayer(layer2(state))
      lines <- state.get
    } yield lines == List("Hi!", "Bonjour!")

  // this one depends on config layer
  val showUsingLayer3: ZIO[Any, Nothing, Unit] = {
    val configLayer = ZLayer.succeed(Config(" some_message", 5))

    // provide config layer to layer 3 using the >>> operator
    showTwo("Hi").provideLayer(configLayer >>> layer3)

    // can also do this; I guess provideSomeLayer gives you option to leave out the layer until later
    showTwo("Hi")
      .provideSomeLayer[Has[Config]](layer3)
      .provideLayer(configLayer)
  }

  unsafeRun(showUsingLayer1)
  println(unsafeRun(showUsingLayer2))
  unsafeRun(showUsingLayer3)


  // improve layer 2
  // currently, layer2 requires an initial state; but this could be refactored as another layer
  // lets introduce new service called lines persistence
  type Lines = List[String]
  trait LinesPersistence {
    // takes previous state to construct new state
    def update(toState: Lines => Lines): ZIO[Any, Nothing, Unit]

    // return current state, which is the lines
    def get: ZIO[Any, Nothing, Lines]
  }

  object LinesPersistence {
    case class LinesPersistenceUsingRef(lines: zio.Ref[List[String]]) extends LinesPersistence {
      override def update(toState: Lines => Lines): ZIO[Any, Nothing, Unit] = lines.update(toState)

      override def get: ZIO[Any, Nothing, Lines] = lines.get
    }

    // two ways of doing same thing
    val layer: ZLayer[Any, Nothing, Has[LinesPersistence]] =
      ZLayer.fromEffect(zio.Ref.make(List.empty[String]).map(LinesPersistenceUsingRef))
      zio.Ref.make(List.empty[String]).map(LinesPersistenceUsingRef).toLayer

    // two ways of doing same thing
    //def layer2[A]: ZLayer[Any, Nothing, Has[A]] =
  }

  case class ShowTestTwo(lines: LinesPersistence) extends Show {
    override def display(message: String): ZIO[Any, Nothing, Unit] =
      lines.update(_ :+ message)
  }

  val layer2Two: ZLayer[Has[LinesPersistence], Nothing, Has[Show]] =
    ZLayer.fromService(ShowTestTwo) // ShowTestTwo is a function from LinesPersistance => Show

  case class testClass(i: Int)
  def testFunc[A, B](a: A, f: A => B): B = f(a)
  testFunc(5, testClass) // this works; case class can be `A => B`


  val showUsingModifiedLayerTwo: ZIO[Any, Nothing, Boolean] =
    (for {
      state <- ZIO.service[LinesPersistence]
      _     <- (showTwo("Hi!") *> showTwo("Bonjour!"))
      lines <- state.get // using get method of lines persistance
    } yield lines == List("Hi!", "Bonjour!"))
      // this doesn't compile
      // because LinesPersistance is required for ZIO.service as well; need to provide it twice
      //.provideLayer(LinesPersistence.layer >>> layer2Two)
      // need to use >+> which provides it internally
      .provideLayer(LinesPersistence.layer >+> layer2Two)

  print(unsafeRun(showUsingModifiedLayerTwo))
}
