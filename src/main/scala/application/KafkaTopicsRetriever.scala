package application

import scala.util.matching.Regex

object KafkaTopicsRetriever {

  trait Service {
    def retrieveKafkaTopics(topicRegex: Regex)
  }

}
