#pragma once

#include "LogTypes.h"
#include "ArgumentParser.h"

#include <string>
#include <map>
#include <optional>

using namespace Utilities;

class Configurations
{
public:
	Configurations(ArgumentParser&& arguments);
	virtual ~Configurations(void);

	auto write_file() -> LogTypes;
	auto encrypt_mode() -> bool;
	auto write_console() -> LogTypes;
	auto console_windows() -> bool;

	auto high_priority_count() -> uint16_t;
	auto normal_priority_count() -> uint16_t;
	auto low_priority_count() -> uint16_t;
	auto write_interval() -> uint16_t;

	auto client_title() -> std::string;
	auto log_root_path() -> std::string;

	auto buffer_size() -> std::size_t;
	auto server_ip() -> std::string;
	auto server_port() -> uint16_t;

	auto use_ssl() -> bool;
	auto ca_cert() -> std::string;
	auto engine() -> std::string;
	auto client_cert() -> std::string;
	auto client_key() -> std::string;

	auto kafka_host() -> std::string;
	auto kafka_brokers() -> std::vector<std::tuple<std::string, std::string>>;
	auto kafka_port() -> uint16_t;
	auto kafka_topic_name() -> std::string;
	auto kafka_topic_group_name() -> std::string;
	auto kafka_linger_ms() -> int;
	auto kafka_batch_size() -> int;
	auto kafka_security_protocol() -> std::string;
	auto kafka_acks() -> std::string;
	auto kafka_retries() -> int;
	auto kafka_compression_type() -> std::string;
	auto kafka_enable_idempotence() -> bool;

protected:
	auto load() -> void;
	auto parse(ArgumentParser& arguments) -> void;

private:
	LogTypes write_file_;
	LogTypes write_console_;
	LogTypes callback_message_log_;
	bool console_windows_;
	bool encrypt_mode_;

	uint16_t high_priority_count_;
	uint16_t normal_priority_count_;
	uint16_t low_priority_count_;
	uint16_t write_interval_;

	std::string client_title_;
	std::string root_path_;
	std::string log_root_path_;

	std::size_t buffer_size_;
	std::string server_ip_;
	uint16_t server_port_;

	std::string kafka_host_;
	std::vector<std::tuple<std::string, std::string>> kafka_brokers_;
	int kafka_port_;
	std::string kafka_topic_name_;
	std::string kafka_topic_group_name_;

	bool use_ssl_;
	std::string ca_cert_;
	std::string engine_;
	std::string client_cert_;
	std::string client_key_;

	int kafka_linger_ms_;
	int kafka_batch_size_;
	std::string kafka_security_protocol_;
	std::string kafka_acks_;
	int kafka_retries_;
	std::string kafka_compression_type_;
	bool kafka_enable_idempotence_;

};