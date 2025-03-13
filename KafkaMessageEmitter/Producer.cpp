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
        std::string brokers = fmt::format("{}:{}", configurations_->kafka_host(), configurations_->kafka_port());
        Kafka::KafkaConfig kafka_config(brokers);
        kafka_config.add_config("linger.ms", "5");
        kafka_config.add_config("batch.size", "32768");
        kafka_config.add_config("security.protocol", "SASL_SSL");

        // TODO
        // TLS/SSL settings

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

        auto delivery_result = kafka_queue_emitter_->send(kafka_message);

        if (delivery_result.get_status() == Kafka::DeliveryResult::Status::Failed)
        {
            Logger::handle().write(LogTypes::Error, fmt::format("failed send message (kafka) = {}", delivery_result.get_message()));
            if (delivery_result.get_error().has_value())
            {
                Logger::handle().write(LogTypes::Error, fmt::format("kafka producer send error = {}", delivery_result.get_error().value()));
            }
            return { false, delivery_result.get_error().value() };
        }

        return { true, std::nullopt };
    }

    auto Producer::send_message(const std::vector<Kafka::KafkaMessage> &kafka_messages) -> std::tuple<bool, std::optional<std::string>>
    {
        if (kafka_queue_emitter_ == nullptr)
        {
            Logger::handle().write(LogTypes::Error, "KafkaQueueEmitter is not initialized.");
            return { false, "KafkaQueueEmitter is not initialized." };
        }

        auto delivery_results = kafka_queue_emitter_->send_batch(kafka_messages);

        for (const auto& delivery_result : delivery_results)
        {
            if (delivery_result.get_status() == Kafka::DeliveryResult::Status::Failed)
            {
                Logger::handle().write(LogTypes::Error, fmt::format("failed send message (kafka) = {}", delivery_result.get_message()));
                if (delivery_result.get_error().has_value())
                {
                    Logger::handle().write(LogTypes::Error, fmt::format("kafka producer send error = {}", delivery_result.get_error().value()));
                }
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
}