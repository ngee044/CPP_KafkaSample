#pragma once

#include "KafkaQueueEmitter.h"
#include "Logger.h"

#include <memory>
#include <string>
#include <tuple>
#include <optional>


namespace KafkaMessageConsumer
{
	class DLQProducer
	{
	public:
		DLQProducer(const std::string& brokers, const std::string& dlq_topic);
		~DLQProducer();

		auto start() -> std::tuple<bool, std::optional<std::string>>;
		auto stop() -> void;
		
		auto send_to_dlq(const Kafka::KafkaMessage& message) -> std::tuple<bool, std::optional<std::string>>;

	private:
		std::shared_ptr<Kafka::KafkaQueueEmitter> kafka_queue_emitter_;
		std::string brokers_;
		std::string dlq_topic_;
	};
}