#pragma once

#include "KafkaQueueEmitter.h"
#include "Configurations.h"
#include "ThreadPool.h"

#include <string>
#include <memory>
#include <tuple>
#include <optional>

using namespace Thread;

namespace KafkaMessageEmitter
{
    class Producer
    {
    public:
        Producer(std::shared_ptr<Configurations> configurations);
        virtual ~Producer();

        auto start() -> std::tuple<bool, std::optional<std::string>>;
        auto stop() -> void;

        auto send_message(const KafkaMessage& kafka_message) -> std::tuple<bool, std::optional<std::string>>;

    protected:
        auto create_thread_pool() -> std::tuple<bool, std::optional<std::string>>;
        auto destroy_thread_pool() -> void;

    private:
        std::shared_ptr<Configurations> configurations_;
        std::shared_ptr<Kafka::KafkaQueueEmitter> kafka_queue_emitter_;
        std::shared_ptr<ThreadPool> thread_pool_;
    };
}