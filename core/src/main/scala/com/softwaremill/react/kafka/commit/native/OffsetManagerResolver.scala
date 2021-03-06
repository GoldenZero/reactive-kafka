package com.softwaremill.react.kafka.commit.native

import kafka.api.{GroupCoordinatorResponse, GroupCoordinatorRequest}
import kafka.cluster.{BrokerEndPoint, Broker}
import kafka.common.ErrorMapping
import kafka.consumer.KafkaConsumer
import kafka.network.BlockingChannel
import org.apache.kafka.common.protocol.SecurityProtocol

import scala.util.{Failure, Success, Try}

/**
 * Responsible for finding current offset manager in the cluster.
 * NOT THREAD SAFE.
 */
private[native] class OffsetManagerResolver(
    blockingChannelFactory: (String, Int) => BlockingChannel = KafkaChannelFactory,
    channelMetadataReader: (BlockingChannel, GroupCoordinatorRequest) => GroupCoordinatorResponse = KafkaChannelReader
) {

  var correlationId = 0

  def resolve(
    kafkaConsumer: KafkaConsumer[_],
    retriesLeft: Int = OffsetManagerResolver.MaxRetries,
    lastErrorOpt: Option[Throwable] = None
  ): Try[BlockingChannel] = {
    if (retriesLeft == 0)
      Failure(lastErrorOpt.map(new OffsetManagerResolvingException(_)).getOrElse(
        new IllegalStateException("Could not resolve offset manager")
      ))
    else
      tryGetChannel(kafkaConsumer, retriesLeft)
  }

  private def tryGetChannel(kafkaConsumer: KafkaConsumer[_], retriesLeft: Int): Try[BlockingChannel] = {
    val channelTrial = for {
      initialChannel <- connectToAnyBroker(kafkaConsumer.props.brokerList)
      coordinator <- getCoordinator(initialChannel, kafkaConsumer)
      finalChannel <- coordinatorOrInitialChannel(coordinator, initialChannel)
    } yield finalChannel

    if (channelTrial.isFailure) {
      Thread.sleep(OffsetManagerResolver.RetryIntervalMs)
      channelTrial.failed.flatMap(err => resolve(kafkaConsumer, retriesLeft - 1, Some(err)))
    }
    else channelTrial
  }

  private def coordinatorOrInitialChannel(
    coordinator: BrokerEndPoint,
    initialChannel: BlockingChannel
  ): Try[BlockingChannel] = {
    if (coordinator.host == initialChannel.host && coordinator.port == initialChannel.port)
      Success(initialChannel)
    else
      connectToChannel(BrokerLocation(coordinator.host, coordinator.port))
  }

  private def connectToAnyBroker(brokerList: String) = {
    val brokers = extractAllBrokers(brokerList)
    val firstBrokerTrial = brokers.headOption.map(Success(_)).getOrElse(
      Failure(new IllegalStateException(s"No brokers in list: $brokerList"))
    )
    for {
      broker <- firstBrokerTrial
      channel <- connectToChannel(broker)
    } yield channel
  }

  private def connectToChannel(location: BrokerLocation): Try[BlockingChannel] = {
    Try {
      val channel = blockingChannelFactory(location.host, location.port)
      channel.connect()
      channel
    }
  }

  private def extractAllBrokers(brokerList: String): Vector[BrokerLocation] = {
    brokerList.split(',').map {
      elem =>
        val Array(hostStr, portStr) = elem.split(':')
        BrokerLocation(hostStr, portStr.toInt)
    }.toVector
  }

  private def getCoordinator(channel: BlockingChannel, consumer: KafkaConsumer[_]): Try[BrokerEndPoint] = {
    correlationId = correlationId + 1
    val group = consumer.props.groupId
    val request = new GroupCoordinatorRequest(group, GroupCoordinatorRequest.CurrentVersion, correlationId)
    Try(channelMetadataReader(channel, request)).flatMap { metadataResponse =>
      if (metadataResponse.errorCode == ErrorMapping.NoError)
        metadataResponse.coordinatorOpt.map(Success(_)).getOrElse(Failure(new IllegalStateException("Missing coordinator")))
      else
        Failure(new IllegalStateException(s"Cannot connect to coordinator. Error code: ${metadataResponse.errorCode}"))
    }
  }
}

private[native] object OffsetManagerResolver {
  val ChannelReadTimeoutMs = 5000
  val MaxRetries = 3
  val RetryIntervalMs = 400L
}

private[native] case class BrokerLocation(host: String, port: Int)

private[native] case class OffsetManagerResolvingException(cause: Throwable)
  extends Exception("Could not resolve offset manager", cause)

private[native] object KafkaChannelFactory extends ((String, Int) => BlockingChannel) {
  override def apply(host: String, port: Int): BlockingChannel = {
    new BlockingChannel(
      host,
      port,
      BlockingChannel.UseDefaultBufferSize,
      BlockingChannel.UseDefaultBufferSize,
      OffsetManagerResolver.ChannelReadTimeoutMs
    )
  }
}

private[native] object KafkaChannelReader
    extends ((BlockingChannel, GroupCoordinatorRequest) => GroupCoordinatorResponse) {
  override def apply(channel: BlockingChannel, request: GroupCoordinatorRequest): GroupCoordinatorResponse = {
    channel.send(request)
    GroupCoordinatorResponse.readFrom(channel.receive().payload())
  }
}