
#include "Logger.h"
#include "Configurations.h"
#include "Consumer.h"

#include "fmt/format.h"
#include "fmt/xchar.h"

#include <iostream>
#include <signal.h>

using namespace Utilities;
using namespace KafkaMessageConsumer;

void register_signal(void);
void deregister_signal(void);
void signal_callback(int32_t signum);

std::shared_ptr<Configurations> configurations_ = nullptr;
std::shared_ptr<Consumer> kafka_consumer_ = nullptr;


int main(int argc, char* argv[]) 
{
	configurations_ = std::make_shared<Configurations>(ArgumentParser(argc, argv));

	Logger::handle().file_mode(configurations_->write_file());
	Logger::handle().console_mode(configurations_->write_console());
	Logger::handle().write_interval(configurations_->write_interval());
	Logger::handle().log_root(configurations_->log_root_path());

	Logger::handle().start(configurations_->client_title());

	kafka_consumer_ = std::make_shared<Consumer>(configurations_);

	auto [result, message] = kafka_consumer_->start();
	if (!result)
	{
		Logger::handle().write(LogTypes::Error, fmt::format("Failed to start consumer: {}", message.value()));
		return 1;
	}

	Logger::handle().write(LogTypes::Information, "KafkaMessageConsumer started.");
	
	kafka_consumer_->wait_stop();

	kafka_consumer_.reset();
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