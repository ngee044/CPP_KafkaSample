#include "Producer.h"

#include "Logger.h"
#include "Job.h"
#include "JobPriorities.h"
#include "ThreadWorker.h"
#include "JobPool.h"
#include "KafkaConfig.h"

#include "fmt/format.h"
#include "fmt/xchar.h"

using namespace Utilities;

namespace KafkaMessageEmitter
{
    Producer::Producer(std::shared_ptr<Configurations> configurations)
        : configurations_(configurations)
        , kafka_queue_emitter_(nullptr)
    {
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
        kafka_config.add_config("acks", configurations_->kafka_acks());
        kafka_config.add_config("retries", std::to_string(configurations_->kafka_retries()));
        kafka_config.add_config("compression.type", configurations_->kafka_compression_type());
        kafka_config.add_config("enable.idempotence", configurations_->kafka_enable_idempotence() ? "true" : "false");
        kafka_config.add_config("linger.ms", "5");
        kafka_config.add_config("batch.size", "32768");
        kafka_config.add_config("security.protocol", configurations_->kafka_security_protocol());

        if (configurations_->use_ssl())
        {
            kafka_config.add_config("ssl.ca.location", configurations_->ca_cert());
            kafka_config.add_config("ssl.engine", configurations_->engine());
            kafka_config.add_config("ssl.certificate.location", configurations_->client_cert());
            kafka_config.add_config("ssl.key.location", configurations_->client_key());
        }

        kafka_queue_emitter_ = std::make_shared<Kafka::KafkaQueueEmitter>(kafka_config);
    }

    Producer::~Producer()
    {
        stop();
    }

    auto Producer::start() -> std::tuple<bool, std::optional<std::string>>
    {
        if (kafka_queue_emitter_ == nullptr)
        {
            Logger::handle().write(LogTypes::Error, "KafkaQueueEmitter is not initialized.");
            return { false, "KafkaQueueEmitter is not initialized." };
        }

        auto [result, error] = create_thread_pool();
        if (!result)
        {
            Logger::handle().write(LogTypes::Error, fmt::format("Failed to create thread pool: {}", error.value()));
            return { false, error };
        }

        auto [start_ok, start_error] = kafka_queue_emitter_->start();
        if (!start_ok)
        {
            Logger::handle().write(LogTypes::Error, fmt::format("KafkaQueueEmitter start failed: {}", start_error.value()));
            return { false, start_error };
        }

        return { true, std::nullopt };
    }

    auto Producer::stop() -> void
    {
        if (kafka_queue_emitter_ != nullptr)
        {
            kafka_queue_emitter_->flush();
            kafka_queue_emitter_->stop();
        }

        destroy_thread_pool();
    }

    auto Producer::send_message(const Kafka::KafkaMessage &kafka_message) -> std::tuple<bool, std::optional<std::string>>
    {
        if (kafka_queue_emitter_ == nullptr)
        {
            Logger::handle().write(LogTypes::Error, "KafkaQueueEmitter is not initialized.");
            return { false, "KafkaQueueEmitter is not initialized." };
        }
        
        return retry_send(kafka_message);
    }

    auto Producer::send_message(const std::vector<Kafka::KafkaMessage> &kafka_messages) -> std::tuple<bool, std::optional<std::string>>
    {
        if (kafka_queue_emitter_ == nullptr)
        {
            Logger::handle().write(LogTypes::Error, "KafkaQueueEmitter is not initialized.");
            return { false, "KafkaQueueEmitter is not initialized." };
        }

        for (const auto& message : kafka_messages)
        {
            auto [result, error] = retry_send(message);
            if (!result)
            {
                Logger::handle().write(LogTypes::Error, fmt::format("Failed to send message: {}", error.value()));
                return { false, error };
            }
        }

        return { true, std::nullopt };
    }

    auto Producer::create_thread_pool() -> std::tuple<bool, std::optional<std::string>>
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

    auto Producer::destroy_thread_pool() -> void
    {
        if (thread_pool_ == nullptr)
		{
			return;
		}
	
		thread_pool_->stop();
		thread_pool_.reset();
    }
 
    auto Producer::retry_send(const Kafka::KafkaMessage &message) -> std::tuple<bool, std::optional<std::string>>
    {
        if (kafka_queue_emitter_ == nullptr)
        {
            Logger::handle().write(LogTypes::Error, "KafkaQueueEmitter is not initialized.");
            return { false, "KafkaQueueEmitter is not initialized." };
        }

        int retries = configurations_->kafka_retries();
        if (retries <= 0)
        {
            retries = 3;
        }
        for (auto i = 0; i < retries ; i++)
        {
            auto delivery_result = kafka_queue_emitter_->send(message);
            if (delivery_result.get_status() == Kafka::DeliveryResult::Status::Success)
            {
                return { true, std::nullopt };
            }
            
            Logger::handle().write(LogTypes::Error, fmt::format("failed send message (kafka) = {}", delivery_result.get_message()));
            if (delivery_result.get_error().has_value())
            {
                Logger::handle().write(LogTypes::Error, fmt::format("kafka producer send error = {}", delivery_result.get_error().value()));
            }

            std::this_thread::sleep_for(std::chrono::seconds(i));
        }
        return { false, "Failed to send message to kafka" };
    }
}