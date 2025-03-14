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
		auto wait_stop() -> std::tuple<bool, std::optional<std::string>>;

		auto registered_brokers() -> std::map<std::string, std::string> { return registered_brokers_; }

	protected:
		auto message_polling() -> std::tuple<bool, std::optional<std::string>>;

        // error handle, with retry(dql)
		auto handle_message_dlq(const Kafka::KafkaMessage& message) -> std::tuple<bool, std::optional<std::string>>;
		auto send_to_dlq(const Kafka::KafkaMessage& message) -> void;

		auto create_thread_pool() -> std::tuple<bool, std::optional<std::string>>;
		auto destroy_thread_pool() -> void;

	private:
		std::shared_ptr<Configurations> configurations_;
		std::shared_ptr<Kafka::KafkaQueueConsume> kafka_queue_consume_;
		std::shared_ptr<ThreadPool> thread_pool_;
		std::map<std::string, std::string> registered_brokers_;

		std::atomic<bool> running_;
		std::thread polling_thread_;

		size_t messages_since_commit_;
		std::chrono::time_point<std::chrono::system_clock> last_commit_time_;
	};

}