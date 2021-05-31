import zio.Has

package object demo {

  final type MainEnv = Has[MainEnv.Service]
}
