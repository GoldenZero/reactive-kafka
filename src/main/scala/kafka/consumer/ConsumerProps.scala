package kafka.consumer

import java.util.Properties
import java.util.UUID

object ConsumerProps {

  /**
   * Consumer Properties
   *
   * brokerList
   * This is for bootstrapping and the producer will only use it for getting metadata (topics, partitions and replicas).
   * The socket connections for sending the actual data will be established based on the broker information returned in
   * the metadata. The format is host1:port1,host2:port2, and the list can be a subset of brokers or a VIP pointing to a
   * subset of brokers.
   *
   * zooKeeperHost
   * Specifies the zookeeper connection string in the form hostname:port where host and port are the host and port of
   * a zookeeper server. To allow connecting through other zookeeper nodes when that zookeeper machine is down you can also
   * specify multiple hosts in the form hostname1:port1,hostname2:port2,hostname3:port3. The server may also have a zookeeper
   * chroot path as part of it's zookeeper connection string which puts its data under some path in the global zookeeper namespace.
   * If so the consumer should use the same chroot path in its connection string. For example to give a chroot path of /chroot/path
   * you would give the connection string as hostname1:port1,hostname2:port2,hostname3:port3/chroot/path.
   *
   * topic
   * The high-level API hides the details of brokers from the consumer and allows consuming off the cluster of machines
   * without concern for the underlying topology. It also maintains the state of what has been consumed. The high-level API
   * also provides the ability to subscribe to topics that match a filter expression (i.e., either a whitelist or a blacklist
   * regular expression).  This topic is a whitelist only but can change with re-factoring below on the filterSpec
   *
   * groupId
   * A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the same
   * group id multiple processes indicate that they are all part of the same consumer group.
   *
   */
  def apply(brokerList: String, zooKeeperHost: String, topic: String, groupId: String = UUID.randomUUID().toString): ConsumerProps = {
    val props = Map[String, String](
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupId,
      "zookeeper.connect" -> zooKeeperHost,

      // defaults
      "auto.offset.reset" -> "smallest",
      "consumer.timeout.ms" -> "1500",
      "offsets.storage" -> "zookeeper"
    )

    new ConsumerProps(props, topic, groupId)
  }
}

case class ConsumerProps(private val params: Map[String, String], topic: String, groupId: String) {

  /**
   * Consumer Timeout
   * Throw a timeout exception to the consumer if no message is available for consumption after the specified interval
   */
  def consumerTimeoutMs(timeInMs: Long): ConsumerProps = {
    ConsumerProps(params + ("consumer.timeout.ms" -> timeInMs.toString), topic, groupId)
  }

  /**
   * What to do when there is no initial offset in Zookeeper or if an offset is out of range:
   * 1) smallest : automatically reset the offset to the smallest offset
   * 2) largest : automatically reset the offset to the largest offset
   * 3) anything else: throw exception to the consumer. If this is set to largest, the consumer may lose some
   * messages when the number of partitions, for the topics it subscribes to, changes on the broker.
   *
   * ***************************************************************************************
   * To prevent data loss during partition addition, set auto.offset.reset to smallest
   *
   * This make sense to change to true if you know you are listening for new data only as of
   * after you connect to the stream new things are coming out.  you can audit/reconcile in
   * another consumer which this flag allows you to toggle if it is catch-up and new stuff or
   * just new stuff coming out of the stream.  This will also block waiting for new stuff so
   * it makes a good listener.
   *
   * //readFromStartOfStream: Boolean = true
   * readFromStartOfStream: Boolean = false
   * ***************************************************************************************
   *
   */
  def readFromEndOfStream(): ConsumerProps = {
    ConsumerProps(params + ("auto.offset.reset" -> "largest"), topic, groupId)
  }

  /**
   * Store offsets in Kafka and/or ZooKeeper. NOTE: Server instance must be 8.2 or higher
   *
   * dualCommit = true means store in both ZooKeeper(legacy) and Kafka(new) places.
   */
  def kafkaOffsetsStorage(dualCommit: Boolean): ConsumerProps = {
    val p = params + (
      "offsets.storage" -> "kafka",
      "dual.commit.enabled" -> dualCommit.toString
    )
    ConsumerProps(p, topic, groupId)
  }
  /**
   * Set any additional properties as needed
   */
  def setProperty(key: String, value: String): ConsumerProps = ConsumerProps(params + (key -> value), topic, groupId)
  def setProperties(values: (String, String)*): ConsumerProps = ConsumerProps(params ++ values, topic, groupId)

  /**
   *  Generate the Kafka ConsumerConfig object
   *
   */
  def toConsumerConfig: ConsumerConfig = {
    new ConsumerConfig(params.foldLeft(new Properties()) { (props, param) => props.put(param._1, param._2); props })
  }

  // accessors
  def zookeeperConnect: Option[String] = params.get("zookeeper.connect")

  /**
   * Dump current props for debugging
   */
  def dump: String = params.map { e => f"${e._1}%-20s : ${e._2.toString}" }.mkString("\n")
}