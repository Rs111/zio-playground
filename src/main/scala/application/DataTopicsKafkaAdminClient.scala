package application

import application.model.Topic
import zio.{Has, RIO, Task, ZIO, ZLayer}

object DataTopicsKafkaAdminClient {

  trait Service {
    def getAllNonInternalTopics: Task[List[Topic]]
  }

}


