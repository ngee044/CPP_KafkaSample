#pragma once

#include "Configurations.h"
#include "KafkaQueueConsume.h"
#include "ThreadPool.h"

#include <string>
#include <memory>
#include <tuple>
#include <optional>

using namespace Thread;

namespace KafkaMessageConsumer
{
	class Consumer
	{
	public:
		Consumer(std::shared_ptr<Configurations> configurations);
		virtual ~Consumer();

		auto start() -> std::tuple<bool, std::optional<std::string>>;
		auto stop() -> void;

	protected:
		auto message_polling() -> std::tuple<bool, std::optional<std::string>>;
		auto create_thread_pool() -> std::tuple<bool, std::optional<std::string>>;
		auto destroy_thread_pool() -> void;

	private:
		std::shared_ptr<Configurations> configurations_;
		std::shared_ptr<Kafka::KafkaQueueConsume> kafka_queue_consume_;
		std::shared_ptr<ThreadPool> thread_pool_;
	};

}