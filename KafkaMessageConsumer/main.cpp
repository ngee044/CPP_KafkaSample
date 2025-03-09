
#include "Logger.h"

#include "fmt/format.h"
#include "fmt/xchar.h"

#include <iostream>
#include <signal.h>

using namespace Utilities;


void register_signal(void);
void deregister_signal(void);
void signal_callback(int32_t signum);

int main(int argc, char* argv[]) {
	std::cout << "Hello, World!" << std::endl;
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