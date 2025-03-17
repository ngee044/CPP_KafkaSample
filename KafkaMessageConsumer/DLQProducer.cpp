#include "DLQProducer.h"

#include "Logger.h"

#include "fmt/format.h"
#include "fmt/xchar.h"

using namespace Utilities;

namespace KafkaMessageConsumer
{
	DLQProducer::DLQProducer(const std::string& brokers, const std::string& dlq_topic)
		: brokers_(brokers)
		, dlq_topic_(dlq_topic)
		, kafka_queue_emitter_(nullptr)
	{
		
	}

	DLQProducer::~DLQProducer()
	{
		stop();
	}

	auto DLQProducer::start() -> std::tuple<bool, std::optional<std::string>>
	{
		Kafka::KafkaConfig config(brokers_);

		// TODO
		// kafka config class settings
		config.add_config("acks", "all");
        config.add_config("retries", "5");

		kafka_queue_emitter_ = std::make_shared<Kafka::KafkaQueueEmitter>(config);

		auto [success, error] = kafka_queue_emitter_->start();
		if (!success)
		{
			Logger::handle().write(LogTypes::Error, fmt::format("Failed to start DLQ Producer: {}", error.value()));
			return { false, error };
		}

		Logger::handle().write(LogTypes::Information, "DLQ Producer started");
		return { true, std::nullopt };
	}

	auto DLQProducer::stop() -> void
	{
		if (kafka_queue_emitter_)
		{
			kafka_queue_emitter_->stop();
			kafka_queue_emitter_.reset();
		}
	}

	auto DLQProducer::send_to_dlq(const Kafka::KafkaMessage& message) -> std::tuple<bool, std::optional<std::string>>
	{
		if (kafka_queue_emitter_ == nullptr)
		{
			Logger::handle().write(LogTypes::Error, "DLQ Producer is not started");
			return { false, "DLQ Producer is not started" };
		}

		auto delivery_result = kafka_queue_emitter_->send(message);
		if (delivery_result.get_status() == Kafka::DeliveryResult::Status::Success)
		{
			return { true, std::nullopt };
		}

		Logger::handle().write(LogTypes::Error, fmt::format("Failed to send message to DLQ message: {}", delivery_result.get_message()));
		if (delivery_result.get_error().has_value())
		{
			Logger::handle().write(LogTypes::Error, fmt::format("Failed to send message to DLQ error: {}", delivery_result.get_error().value()));
		}

		return { false, delivery_result.get_message() };
	}

}