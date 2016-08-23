package pl.allegro.tech.hermes.consumers.consumer.offset.kafka.broker;

import com.google.common.net.HostAndPort;
import kafka.api.GroupCoordinatorRequest;
import kafka.api.GroupCoordinatorResponse;
import kafka.api.TopicMetadataRequest;
import kafka.cluster.Broker;
import kafka.cluster.BrokerEndPoint;
import kafka.common.ErrorMapping;
import kafka.network.BlockingChannel;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.kafka.ConsumerGroupId;
import scala.Option;

import javax.inject.Inject;
import java.util.Arrays;

public class BlockingChannelFactory {

    private final HostAndPort broker;
    private final int readTimeout;
    private final String clientId;

    @Inject
    public BlockingChannelFactory(ConfigFactory configFactory) {
        this(HostAndPort.fromString(findAnyBroker(configFactory.getStringProperty(Configs.KAFKA_BROKER_LIST))),
                configFactory.getIntProperty(Configs.KAFKA_CONSUMER_METADATA_READ_TIMEOUT),
                configFactory.getStringProperty(Configs.CONSUMER_CLIENT_ID));
    }

    private static String findAnyBroker(String brokerList) {
        return Arrays.stream(brokerList.split(",")).findAny().get();
    }

    public BlockingChannelFactory(HostAndPort broker, int readTimeout, String clientId) {
        this.broker = broker;
        this.readTimeout = readTimeout;
        this.clientId = clientId;
    }

    public BlockingChannel create(ConsumerGroupId consumerGroupId) {
        GroupCoordinatorResponse metadataResponse = readConsumerMetadata(consumerGroupId);

        if (metadataResponse.errorCode() != ErrorMapping.NoError()) {
            throw new ReadingConsumerMetadataException(metadataResponse.errorCode());
        }

        BrokerEndPoint coordinator = metadataResponse.coordinatorOpt().get();

        BlockingChannel blockingChannel = new BlockingChannel(coordinator.host(), coordinator.port(),
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                readTimeout);

        return blockingChannel;
    }

    private GroupCoordinatorResponse readConsumerMetadata(ConsumerGroupId consumerGroupId) {
        BlockingChannel channel = new BlockingChannel(broker.getHostText(), broker.getPort(),
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                readTimeout);

        channel.connect();
        channel.send(new GroupCoordinatorRequest(consumerGroupId.asString(), GroupCoordinatorRequest.CurrentVersion(), 0, clientId));
        GroupCoordinatorResponse metadataResponse = GroupCoordinatorResponse.readFrom(channel.receive().payload());

        channel.disconnect();
        return metadataResponse;
    }
}
