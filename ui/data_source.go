package ui

import (
	"context"

	kafkaesque_brokers "github.com/nikhil25803/kafkaesque/internals/brokers"
	kafkaesque_config "github.com/nikhil25803/kafkaesque/internals/config"
	kafkaesque_consumers "github.com/nikhil25803/kafkaesque/internals/consumers"
	kafkaesque_health "github.com/nikhil25803/kafkaesque/internals/health"
	kafkaesque "github.com/nikhil25803/kafkaesque/internals/kafka"
	kafkaesque_topics "github.com/nikhil25803/kafkaesque/internals/topics"
)

type dashboardData struct {
	Snapshot *kafkaesque.MetadataSnapshot
	Groups   []kafkaesque_consumers.ConsumerGroupInformation
	Health   *kafkaesque_health.Information
}

type groupDetailData struct {
	Group         *kafkaesque_consumers.ConsumerGroupInformation
	Topic         *kafkaesque_consumers.ConsumerGroupTopicInformation
	SelectedTopic string
}

type dataSource interface {
	Dashboard(context.Context) (*dashboardData, error)
	ConsumerGroups(context.Context) ([]kafkaesque_consumers.ConsumerGroupInformation, error)
	ConsumerGroup(context.Context, string, string) (*groupDetailData, error)
	Topics(context.Context) ([]kafkaesque_topics.TopicInformation, error)
	Brokers(context.Context) ([]kafkaesque_brokers.BrokerInformation, error)
	Health(context.Context) (*kafkaesque_health.Information, error)
	Protocol() string
	Authentication() string
	Thresholds() kafkaesque_consumers.LagThresholds
}

type liveDataSource struct {
	conn       *kafkaesque.KafkaesqueConn
	config     *kafkaesque_config.Config
	thresholds kafkaesque_consumers.LagThresholds
}

func newLiveDataSource(conn *kafkaesque.KafkaesqueConn, cfg *kafkaesque_config.Config) *liveDataSource {
	return &liveDataSource{conn: conn, config: cfg, thresholds: kafkaesque_consumers.LagThresholds{Warning: cfg.Lag.WarningThreshold, Unhealthy: cfg.Lag.UnhealthyThreshold}}
}

func (s *liveDataSource) snapshot(ctx context.Context) (*kafkaesque.MetadataSnapshot, error) {
	return s.conn.GetMetadataSnapshot(ctx)
}

func (s *liveDataSource) Dashboard(ctx context.Context) (*dashboardData, error) {
	snapshot, err := s.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	groups, err := kafkaesque_consumers.GetConsumerGroupsInformation(s.conn, ctx, snapshot.Metadata, s.thresholds)
	if err != nil {
		return nil, err
	}
	health, err := kafkaesque_health.BuildInformation(snapshot.Metadata, groups, s.thresholds)
	if err != nil {
		return nil, err
	}
	return &dashboardData{Snapshot: snapshot, Groups: groups, Health: health}, nil
}

func (s *liveDataSource) ConsumerGroups(ctx context.Context) ([]kafkaesque_consumers.ConsumerGroupInformation, error) {
	snapshot, err := s.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return kafkaesque_consumers.GetConsumerGroupsInformation(s.conn, ctx, snapshot.Metadata, s.thresholds)
}

func (s *liveDataSource) ConsumerGroup(ctx context.Context, group, topic string) (*groupDetailData, error) {
	snapshot, err := s.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	info, err := kafkaesque_consumers.GetConsumerGroupInformation(s.conn, ctx, snapshot.Metadata, group, s.thresholds, true)
	if err != nil {
		return nil, err
	}
	selected := topic
	if selected == "" && len(info.Topics) > 0 {
		selected = info.Topics[0].Topic
	}
	result := &groupDetailData{Group: info, SelectedTopic: selected}
	if selected != "" {
		result.Topic, err = kafkaesque_consumers.GetConsumerGroupTopicInformation(s.conn, ctx, snapshot.Metadata, group, selected)
	}
	return result, err
}

func (s *liveDataSource) Topics(ctx context.Context) ([]kafkaesque_topics.TopicInformation, error) {
	snapshot, err := s.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return kafkaesque_topics.GetTopicInformation(snapshot.Metadata), nil
}

func (s *liveDataSource) Brokers(ctx context.Context) ([]kafkaesque_brokers.BrokerInformation, error) {
	snapshot, err := s.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	return kafkaesque_brokers.GetBrokerInformation(snapshot.Metadata, s.conn.ResolveBrokerAddress), nil
}

func (s *liveDataSource) Health(ctx context.Context) (*kafkaesque_health.Information, error) {
	data, err := s.Dashboard(ctx)
	if err != nil {
		return nil, err
	}
	return data.Health, nil
}

func (s *liveDataSource) Protocol() string                               { return kafkaesque.SecurityProtocol(s.config.Kafka) }
func (s *liveDataSource) Thresholds() kafkaesque_consumers.LagThresholds { return s.thresholds }
func (s *liveDataSource) Authentication() string {
	sasl := s.config.Kafka.Security.SASL.Mechanism
	mtls := s.config.Kafka.Security.TLS.ClientCertFile != ""
	if sasl != "" && mtls {
		return sasl + " + mTLS"
	}
	if sasl != "" {
		return sasl
	}
	if mtls {
		return "mTLS"
	}
	return "None"
}
