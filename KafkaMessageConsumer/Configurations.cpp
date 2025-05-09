#include "Configurations.h"

#include "File.h"
#include "Logger.h"
#include "Converter.h"

#include "fmt/xchar.h"
#include "fmt/format.h"

#include "boost/json.hpp"
#include "boost/json/parse.hpp"

#include <filesystem>


Configurations::Configurations(ArgumentParser&& arguments)
	: write_file_(LogTypes::None)
	, write_console_(LogTypes::Information)
	, console_windows_(false)
	, callback_message_log_(LogTypes::Error)
	, root_path_("")
	, high_priority_count_(3)
	, normal_priority_count_(3)
	, low_priority_count_(5)
	, write_interval_(1000)
	, log_root_path_("")
	, server_ip_("127.0.0.1")
	, server_port_(9876)
	, buffer_size_(32768)
	, encrypt_mode_(true)
	, kafka_host_("")
	, kafka_port_(9092)
	, kafka_topic_name_("")
	, kafka_dlq_topic_name_("")
	, kafka_topic_group_name_("")
	, kafka_enable_auto_commit_(true)
	, kafka_auto_commit_interval_(1000)
	, kafka_message_polling_interval_(500)
	, kafka_auto_offset_reset_("earliest")
	, kafka_acks_("all")
	, kafka_retries_(5)
	, kafka_compression_type_("gzip")
	, kafka_enable_idempotence_(true)
	, use_ssl_(false)
	, ca_cert_("")
	, engine_("")
	, client_cert_("")
	, client_key_("")

{
	root_path_ = arguments.program_folder();

	load();
	parse(arguments);
}

Configurations::~Configurations(void) {}

auto Configurations::write_file() -> LogTypes { return write_file_; }

auto Configurations::encrypt_mode() -> bool { return false; }

auto Configurations::write_console() -> LogTypes { return write_console_; }

auto Configurations::console_windows() -> bool { return console_windows_; }

auto Configurations::high_priority_count() -> uint16_t { return high_priority_count_; }

auto Configurations::normal_priority_count() -> uint16_t { return normal_priority_count_; }

auto Configurations::low_priority_count() -> uint16_t { return low_priority_count_; }

auto Configurations::write_interval() -> uint16_t { return write_interval_; }

auto Configurations::client_title() -> std::string { return client_title_; }

auto Configurations::log_root_path() -> std::string { return log_root_path_; }

auto Configurations::buffer_size() -> std::size_t { return buffer_size_; }

auto Configurations::server_ip() -> std::string { return server_ip_; }

auto Configurations::server_port() -> uint16_t { return server_port_; }

auto Configurations::kafka_host() -> std::string { return kafka_host_; }

auto Configurations::kafka_brokers() -> std::vector<std::tuple<std::string, std::string>> { return kafka_brokers_; }

auto Configurations::kafka_port() -> uint16_t { return kafka_port_; }

auto Configurations::kafka_topic_name() -> std::string { return kafka_topic_name_; }

auto Configurations::kafka_dlq_topic_name() -> std::string { return kafka_dlq_topic_name_; }

auto Configurations::kafka_topic_group_name() -> std::string { return kafka_topic_group_name_; }

auto Configurations::kafka_enable_auto_commit() -> bool { return kafka_enable_auto_commit_; }

auto Configurations::kafka_auto_commit_interval() -> int { return kafka_auto_commit_interval_; }

auto Configurations::kafka_message_polling_interval() -> int { return kafka_message_polling_interval_; }

auto Configurations::kafka_auto_offset_reset() -> std::string { return kafka_auto_offset_reset_; }

auto Configurations::kafka_acks() -> std::string { return kafka_acks_; }

auto Configurations::kafka_retries() -> int { return kafka_retries_; }

auto Configurations::kafka_compression_type() -> std::string { return kafka_compression_type_; }

auto Configurations::kafka_enable_idempotence() -> bool { return kafka_enable_idempotence_; }

auto Configurations::use_ssl() -> bool { return use_ssl_; }

auto Configurations::ca_cert() -> std::string { return ca_cert_; }

auto Configurations::engine() -> std::string { return engine_; }

auto Configurations::client_cert() -> std::string { return client_cert_; }

auto Configurations::client_key() -> std::string { return client_key_; }

auto Configurations::load() -> void
{
	std::filesystem::path path = root_path_ + "kafka_message_consumer_configurations.json";
	if (!std::filesystem::exists(path))
	{
		Logger::handle().write(LogTypes::Error, fmt::format("Configurations file does not exist: {}", path.string()));
		return;
	}

	File source;
	source.open(fmt::format("{}kafka_message_consumer_configurations.json", root_path_), std::ios::in | std::ios::binary, std::locale(""));
	auto [source_data, error_message] = source.read_bytes();
	if (source_data == std::nullopt)
	{
		Logger::handle().write(LogTypes::Error, error_message.value());
		return;
	}

	boost::json::object message = boost::json::parse(Converter::to_string(source_data.value())).as_object();

	if (message.contains("client_title") && message.at("client_title").is_string())
	{
		client_title_ = message.at("client_title").as_string().data();
	}

	if (message.contains("log_root_path") && message.at("log_root_path").is_string())
	{
		log_root_path_ = message.at("log_root_path").as_string().data();
	}

	if (message.contains("write_file") && message.at("write_file").is_string())
	{
		write_file_ = static_cast<LogTypes>(message.at("write_file_log").as_int64());
	}

	if (message.contains("write_console") && message.at("write_console").is_string())
	{
		write_console_ = static_cast<LogTypes>(message.at("write_console").as_int64());
	}

	if (message.contains("callback_message_log") && message.at("callback_message_log").is_string())
	{
		callback_message_log_ = static_cast<LogTypes>(message.at("callback_message_log").as_int64());
	}

	if (message.contains("console_windows") && message.at("console_windows").is_bool())
	{
		console_windows_ = message.at("console_windows").as_bool();
	}

	if (message.contains("high_priority_count") && message.at("high_priority_count").is_number())
	{
		high_priority_count_ = static_cast<int>(message.at("high_priority_count").as_int64());
	}

	if (message.contains("normal_priority_count") && message.at("normal_priority_count").is_number())
	{
		normal_priority_count_ = static_cast<int>(message.at("normal_priority_count").as_int64());
	}

	if (message.contains("low_priority_count") && message.at("low_priority_count").is_number())
	{
		low_priority_count_ = static_cast<int>(message.at("low_priority_count").as_int64());
	}

	if (message.contains("write_interval") && message.at("write_interval").is_number())
	{
		write_interval_ = static_cast<int>(message.at("write_interval").as_int64());
	}

	if (message.contains("buffer_size") && message.at("buffer_size").is_number())
	{
		buffer_size_ = static_cast<int>(message.at("buffer_size").as_int64());
	}

	if (message.contains("main_server_ip") && message.at("main_server_ip").is_string())
	{
		server_ip_ = message.at("main_server_ip").as_string().data();
	}

	if (message.contains("main_server_port") && message.at("main_server_port").is_number())
	{
		server_port_ = static_cast<int>(message.at("main_server_port").as_int64());
	}

	if (message.contains("encrypt_mode") && message.at("encrypt_mode").is_bool())
	{
		encrypt_mode_ = message.at("encrypt_mode").as_bool();
	}

	if (message.contains("kafka_host") && message.at("kafka_host").is_string())
	{
		kafka_host_ = message.at("kafka_host").as_string().data();
	}

	if (message.contains("kafka_brokers") && message.at("kafka_brokers").is_array())
	{
		for (auto& value : message.at("kafka_brokers").as_array())
		{
			if (!value.is_object())
			{
				Logger::handle().write(LogTypes::Error, "kafka_brokers is not an object.");
				continue;
			}

			auto object = value.as_object();
			if (!object.contains("broker_name") || !object.at("broker_name").is_string())
			{
				Logger::handle().write(LogTypes::Error, "kafka_brokers does not contain broker_name.");
				continue;
			}

			if (!object.contains("broker_host") || !object.at("broker_host").is_string())
			{
				Logger::handle().write(LogTypes::Error, "kafka_brokers does not contain broker_host.");
				continue;
			}
			
			std::string broker_name = object.at("broker_name").as_string().data();
			std::string broker_host = object.at("broker_host").as_string().data();

			Logger::handle().write(LogTypes::Information, fmt::format("[Consumer] kafka_brokers: {} {}", broker_name, broker_host));

			kafka_brokers_.push_back({ broker_name, broker_host });
		}
	}

	if (message.contains("kafka_port") && message.at("kafka_port").is_number())
	{
		kafka_port_ = message.at("kafka_port").as_int64();
	}

	if (message.contains("kafka_topic_name") && message.at("kafka_topic_name").is_string())
	{
		kafka_topic_name_ = message.at("kafka_topic_name").as_string().data();
	}

	if (message.contains("kafka_dlq_topic_name") && message.at("kafka_dlq_topic_name").is_string())
	{
		kafka_dlq_topic_name_ = message.at("kafka_dlq_topic_name").as_string().data();
	}

	if (message.contains("kafka_topic_group_name") && message.at("kafka_topic_group_name").is_string())
	{
		kafka_topic_group_name_ = message.at("kafka_topic_group_name").as_string().data();
	}

	if (message.contains("kafka_enable_auto_commit") && message.at("kafka_enable_auto_commit").is_bool())
	{
		kafka_enable_auto_commit_ = message.at("kafka_enable_auto_commit").as_bool();
	}

	if (message.contains("kafka_auto_commit_interval") && message.at("kafka_auto_commit_interval").is_number())
	{
		kafka_auto_commit_interval_ = message.at("kafka_auto_commit_interval").as_int64();
	}

	if (message.contains("kafka_auto_offset_reset") && message.at("kafka_auto_offset_reset").is_string())
	{
		kafka_auto_offset_reset_ = message.at("kafka_auto_offset_reset").as_string().data();
	}

	if (message.contains("kafka_acks") && message.at("kafka_acks").is_string())
	{
		kafka_acks_ = message.at("kafka_acks").as_string().data();
	}

	if (message.contains("kafka_retries") && message.at("kafka_retries").is_number())
	{
		kafka_retries_ = static_cast<int>(message.at("kafka_retries").as_int64());
	}

	if (message.contains("kafka_compression_type") && message.at("kafka_compression_type").is_string())
	{
		kafka_compression_type_ = message.at("kafka_compression_type").as_string().data();
	}

	if (message.contains("kafka_enable_idempotence") && message.at("kafka_enable_idempotence").is_bool())
	{
		kafka_enable_idempotence_ = message.at("kafka_enable_idempotence").as_bool();
	}

	if (message.contains("use_ssl") && message.at("use_ssl").is_bool())
	{
		use_ssl_ = message.at("use_ssl").as_bool();
	}

	// TODO
	// win, linux ca_cert
	if (message.contains("ca_cert") && message.at("ca_cert").is_string())
	{
		ca_cert_ = message.at("ca_cert").as_string().data();
	}
}

auto Configurations::parse(ArgumentParser& arguments) -> void
{
	auto string_target = arguments.to_string("--client_title");
	if (string_target != std::nullopt)
	{
		client_title_ = string_target.value();
	}

	string_target = arguments.to_string("--log_root_path");
	if (string_target != std::nullopt)
	{
		log_root_path_ = string_target.value();
	}

	auto ushort_target = arguments.to_ushort("--write_interval");
	if (ushort_target != std::nullopt)
	{
		write_interval_ = ushort_target.value();
	}

	auto int_target = arguments.to_int("--write_console_log");
	if (int_target != std::nullopt)
	{
		write_console_ = (LogTypes)int_target.value();
	}

	int_target = arguments.to_int("--write_file_log");
	if (int_target != std::nullopt)
	{
		write_file_ = (LogTypes)int_target.value();
	}
}