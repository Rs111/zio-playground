import zio.Has

package object application {

  final type MainEnv = Has[MainEnv.Service]
  final type MainEnvTwo = Has[MainEnvTwo.Service]

}
