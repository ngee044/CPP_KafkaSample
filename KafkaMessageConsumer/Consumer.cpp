#include "Consumer.h"

#include "Logger.h"
#include "Job.h"
#include "JobPriorities.h"
#include "ThreadWorker.h"
#include "JobPool.h"

#include "fmt/format.h"
#include "fmt/xchar.h"

using namespace Utilities;

namespace KafkaMessageConsumer
{
	Consumer::Consumer(std::shared_ptr<Configurations> configurations)
		: configurations_(configurations)
		, kafka_queue_consume_(nullptr)
	{
		if (configurations_ == nullptr)
		{
			Logger::handle().write(LogTypes::Error, "Configurations is nullptr");
			return;
		}

		std::string brokers;
		if (!configurations_->kafka_brokers().empty())
		{
			std::vector<std::string> broker_host_list;
			for (const auto& broker : configurations_->kafka_brokers())
			{
				auto [broker_name, broker_host] = broker;
				broker_host_list.push_back(broker_host);
				registered_brokers_.insert({ broker_name, broker_host });
			}
			brokers = fmt::format("{}", fmt::join(broker_host_list, ","));
		}
		else
		{
			brokers = fmt::format("{}:{}", configurations_->kafka_host(), configurations_->kafka_port());
		}

		Logger::handle().write(LogTypes::Information, fmt::format("Kafka brokers: {}", brokers));
		
		Kafka::KafkaConfig kafka_config(brokers);
		kafka_config.add_config("enable.auto.commit", configurations_->kafka_enable_auto_commit() ? "true" : "false");
		kafka_config.add_config("auto.commit.interval.ms", std::to_string(configurations_->kafka_auto_commit_interval()));
		kafka_config.add_config("auto.offset.reset", configurations_->kafka_auto_offset_reset());


		kafka_queue_consume_ = std::make_shared<Kafka::KafkaQueueConsume>(kafka_config);

// TODO
// kafka config class settings
#if 0
		kafka_config.set_group_id(configurations_->kafka_topic_group_name());
		kafka_config.set_auto_commit(configurations_->kafka_enable_auto_commit());
		kafka_config.set_timeout_ms(configurations_->kafka_auto_commit_interval());
#endif

	}

	Consumer::~Consumer()
	{
		stop();
	}

	auto Consumer::create_thread_pool() -> std::tuple<bool, std::optional<std::string>>
	{
		destroy_thread_pool();

		try
		{	
			thread_pool_ = std::make_shared<ThreadPool>();
		}
		catch(const std::bad_alloc& e)
		{
			return { false, fmt::format("Memory allocation failed to ThreadPool: {}", e.what()) };
		}
		
		for (auto i = 0; i < configurations_->high_priority_count(); i++)
		{
			std::shared_ptr<ThreadWorker> worker;
			try
			{
				worker = std::make_shared<ThreadWorker>(std::vector<JobPriorities>{ JobPriorities::High });
			}
			catch(const std::bad_alloc& e)
			{
				return { false, fmt::format("Memory allocation failed to ThreadWorker: {}", e.what()) };
			}

			thread_pool_->push(worker);
		}

		for (auto i = 0; i < configurations_->normal_priority_count(); i++)
		{
			std::shared_ptr<ThreadWorker> worker;
			try
			{
				worker = std::make_shared<ThreadWorker>(std::vector<JobPriorities>{ JobPriorities::Normal, JobPriorities::High });
			}
			catch(const std::bad_alloc& e)
			{
				return { false, fmt::format("Memory allocation failed to ThreadWorker: {}", e.what()) };
			}

			thread_pool_->push(worker);
		}

		for (auto i = 0; i < configurations_->low_priority_count(); i++)
		{
			std::shared_ptr<ThreadWorker> worker;
			try
			{
				worker = std::make_shared<ThreadWorker>(std::vector<JobPriorities>{ JobPriorities::Low });
			}
			catch(const std::bad_alloc& e)
			{
				return { false, fmt::format("Memory allocation failed to ThreadWorker: {}", e.what()) };
			}

			thread_pool_->push(worker);
		}

		auto [result, message] = thread_pool_->start();
		if (!result)
		{
			Logger::handle().write(LogTypes::Error, fmt::format("Failed to start thread pool: {}", message.value()));
			return { false, message.value() };
		}

		return { true, std::nullopt };
	}
	
	auto Consumer::destroy_thread_pool() -> void
	{
		if (thread_pool_ == nullptr)
		{
			return;
		}
	
		thread_pool_->stop();
		thread_pool_.reset();
	}

	auto Consumer::start() -> std::tuple<bool, std::optional<std::string>>
	{
		if (kafka_queue_consume_ == nullptr)
		{
			Logger::handle().write(LogTypes::Error, "KafkaQueueConsume is not initialized.");
			return { false, "KafkaQueueConsume is not initialized." };
		}

		auto [result, error] = create_thread_pool();
		if (!result)
		{
			Logger::handle().write(LogTypes::Error, fmt::format("Failed to create thread pool: {}", error.value()));
			return { false, error };
		}

		auto [start_ok, start_error] = kafka_queue_consume_->start();
		if (!start_ok)
		{
			Logger::handle().write(LogTypes::Error, fmt::format("KafkaQueueConsume start failed: {}", start_error.value()));
			return { false, start_error };
		}

		auto [subscribe_ok, subscribe_error] = kafka_queue_consume_->subscribe(configurations_->kafka_topic_name());
		if (!subscribe_ok)
		{
			Logger::handle().write(LogTypes::Error, fmt::format("KafkaQueueConsume subscribe failed: {}", subscribe_error.value()));
			return { false, subscribe_error };
		}

		Logger::handle().write(LogTypes::Information, fmt::format("KafkaQueueConsume subscribed to topic: {}", configurations_->kafka_topic_name()));
	
		thread_pool_->push(std::make_shared<Job>(JobPriorities::High, std::bind(&Consumer::message_polling, this), "message_polling_job"));

		return { true, std::nullopt };
	}

	auto Consumer::stop() -> void
	{
		if (kafka_queue_consume_ != nullptr)
		{
			kafka_queue_consume_->stop();
		}

		destroy_thread_pool();
	}

    auto Consumer::wait_stop() -> std::tuple<bool, std::optional<std::string>>
    {
        if (kafka_queue_consume_ == nullptr)
		{
			Logger::handle().write(LogTypes::Error, "KafkaQueueConsume is not initialized.");
			return { false, "KafkaQueueConsume is not initialized." };
		}

		return kafka_queue_consume_->wait_stop();
    }

    auto Consumer::message_polling() -> std::tuple<bool, std::optional<std::string>>
	{
		if (thread_pool_ == nullptr)
		{
			Logger::handle().write(LogTypes::Error, "ThreadPool is not initialized.");
			return { false, "ThreadPool is not initialized." };
		}

		if (kafka_queue_consume_ == nullptr)
		{
			Logger::handle().write(LogTypes::Error, "KafkaQueueConsume is not initialized.");
			return { false, "KafkaQueueConsume is not initialized." };
		}

		auto messages = kafka_queue_consume_->poll(std::chrono::milliseconds(500));
		for (auto& message : messages)
		{
			// TODO
			// message handling
			Logger::handle().write(LogTypes::Information, fmt::format("KafkaQueueConsume message => topic: {},  key: {}, value: {}", message.topic(), message.key(), message.value()));
		}

		auto job_pool = thread_pool_->job_pool();
		if (job_pool == nullptr || job_pool->lock())
		{
			Logger::handle().write(LogTypes::Error, "job_pool is null");
			return { false, "job_pool is null" };
		}
		
		if (configurations_->kafka_enable_auto_commit())
		{
			kafka_queue_consume_->commit_sync();
		}

		job_pool->push(std::make_shared<Job>(JobPriorities::High, std::bind(&Consumer::message_polling, this), "message_polling_job"));
		
		return { true, std::nullopt };
	}

}
