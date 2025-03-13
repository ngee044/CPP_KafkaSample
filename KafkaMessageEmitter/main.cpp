#include "Configurations.h"
#include "Logger.h"
#include "KafkaMessage.h"
#include "Producer.h"

#include "fmt/format.h"
#include "fmt/xchar.h"

#include "boost/json.hpp"
#include "boost/json/parse.hpp"

#include <iostream>
#include <signal.h>

using namespace Utilities;
using namespace KafkaMessageEmitter;

void register_signal(void);
void deregister_signal(void);
void signal_callback(int32_t signum);

std::shared_ptr<Configurations> configurations_ = nullptr;
std::shared_ptr<Producer> kafka_producer_ = nullptr;

int main(int argc, char* argv[]) 
{
	configurations_ = std::make_shared<Configurations>(ArgumentParser(argc, argv));

	Logger::handle().file_mode(configurations_->write_file());
	Logger::handle().console_mode(configurations_->write_console());
	Logger::handle().write_interval(configurations_->write_interval());
	Logger::handle().log_root(configurations_->log_root_path());

	Logger::handle().start(configurations_->client_title());

	// Multiple instance creation is possible
	kafka_producer_ = std::make_shared<Producer>(configurations_);

	auto [result, message] = kafka_producer_->start();
	if (!result)
	{
		Logger::handle().write(LogTypes::Error, fmt::format("Failed to start producer: {}", message.value()));
		return 1;
	}

	Logger::handle().write(LogTypes::Information, "KafkaMessageEmitter started.");

	boost::json::object send_message =
	{
		{ "name", "kafka_send_message_name"},
		{ "data", "kafka_send_message_data" },
	};
	
	Kafka::KafkaMessage kafka_message(configurations_->kafka_topic_name(), "example_key", boost::json::serialize(send_message));
	kafka_producer_->send_message(kafka_message);

	kafka_producer_.reset();
	configurations_.reset();

	return 0;
}

void register_signal(void)
{
	signal(SIGINT, signal_callback);
	signal(SIGILL, signal_callback);
	signal(SIGABRT, signal_callback);
	signal(SIGFPE, signal_callback);
	signal(SIGSEGV, signal_callback);
	signal(SIGTERM, signal_callback);
}

void deregister_signal(void)
{
	signal(SIGINT, nullptr);
	signal(SIGILL, nullptr);
	signal(SIGABRT, nullptr);
	signal(SIGFPE, nullptr);
	signal(SIGSEGV, nullptr);
	signal(SIGTERM, nullptr);
}

void signal_callback(int32_t signum)
{
	deregister_signal();

	Logger::handle().write(LogTypes::Information, fmt::format("attempt to stop AudioCalculator from signal {}", signum));
}