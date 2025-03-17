#include "Consumer.h"

#include "Logger.h"
#include "Job.h"
#include "JobPriorities.h"
#include "ThreadWorker.h"
#include "JobPool.h"

#include "boost/json.hpp"
#include "boost/json/parse.hpp"

#include "fmt/format.h"
#include "fmt/xchar.h"

using namespace Utilities;

namespace KafkaMessageConsumer
{
	Consumer::Consumer(std::shared_ptr<Configurations> configurations)
		: configurations_(configurations)
		, kafka_queue_consume_(nullptr)
		, running_(false)
        , messages_since_commit_(0)
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
		kafka_config.add_config("acks", configurations_->kafka_acks());
		kafka_config.add_config("retries", std::to_string(configurations_->kafka_retries())); 
		// TODO
		// set compression type
		//kafka_config.add_config("compression.type", configurations_->kafka_compression_type());
		kafka_config.add_config("enable.idempotence", configurations_->kafka_enable_idempotence() ? "true" : "false");

		if (configurations_->use_ssl())
		{
			kafka_config.add_config("security.protocol", "SSL");
			kafka_config.add_config("ssl.ca.location", configurations_->ca_cert());
			kafka_config.add_config("ssl.certificate.location", configurations_->client_cert());
			kafka_config.add_config("ssl.key.location", configurations_->client_key());
		}

		kafka_queue_consume_ = std::make_shared<Kafka::KafkaQueueConsume>(kafka_config);

		dlq_producer_ = std::make_shared<DLQProducer>(brokers, configurations_->kafka_dlq_topic_name());

	}

	Consumer::~Consumer()
	{
		stop();
	}

    auto Consumer::handle_message_dlq(const Kafka::KafkaMessage& message) -> std::tuple<bool, std::optional<std::string>>
    {
		for (auto i = 0; i < configurations_->kafka_retries(); i++)
		{
			try
			{
				// TODO
				// message handling (e.g database, file, etc)
				// if success return true
				// if failed return false

				return { true, std::nullopt };
			}
			catch(const std::exception& e)
			{
				Logger::handle().write(LogTypes::Error, fmt::format("Error processing key={}, attempt {} => {}", message.key(), i, e.what()));
				std::this_thread::sleep_for(std::chrono::seconds(i));
			}
		}

		return { false, "Failed to process message" };
    }

    auto Consumer::send_to_dlq(const Kafka::KafkaMessage &message) -> void
    {
		// TODO
		// send to dlq
		Logger::handle().write(LogTypes::Information, fmt::format("Send to DLQ: topic: {}, key: {}, value: {}", message.topic(), message.key(), message.value()));
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

		auto [dlq_start_ok, dlq_error] = dlq_producer_->start();
		if (!dlq_start_ok)
		{
			Logger::handle().write(LogTypes::Error, fmt::format("DLQ Producer start failed: {}", dlq_error.value()));
			return { false, dlq_error.value() };
		}

		running_.store(true);
		messages_since_commit_ = 0;
		last_commit_time_ = std::chrono::system_clock::now();

		Logger::handle().write(LogTypes::Information, fmt::format("KafkaQueueConsume subscribed to topic: {}", configurations_->kafka_topic_name()));

		// only one thread for long term job (message polling)
		auto worker = std::make_shared<ThreadWorker>(std::vector<JobPriorities>{ JobPriorities::LongTerm });
		thread_pool_->push(worker);
		thread_pool_->push(std::make_shared<Job>(JobPriorities::LongTerm, std::bind(&Consumer::message_polling, this), "message_polling_job"));

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
			auto [success, error] = handle_message_dlq(message);
			if (!success) 
			{
				send_to_dlq(message); 
			}
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
			messages_since_commit_ += messages.size();
			auto now = std::chrono::system_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::seconds>(now - last_commit_time_);
			if (messages_since_commit_ >= configurations_->kafka_message_polling_interval() || duration.count() >= configurations_->kafka_auto_commit_interval())
			{
				kafka_queue_consume_->commit_async();
				messages_since_commit_ = 0;
				last_commit_time_ = now;
			}
		}

		if (!configurations_->kafka_enable_auto_commit())
		{
			
			kafka_queue_consume_->commit_async();
#ifdef _DEBUG
			std::this_thread::sleep_for(std::chrono::seconds(1));
#endif
		}

		job_pool->push(std::make_shared<Job>(JobPriorities::LongTerm, std::bind(&Consumer::message_polling, this), "message_polling_job"));
		
		return { true, std::nullopt };
	}
}
