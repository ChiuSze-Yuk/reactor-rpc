// =============================================================================
// rpc_client.cpp
//
// RPC 客户端：
//   1. 连接到 127.0.0.1:8080（也可通过命令行参数指定 host 和 port）。
//   2. 建立一个测试用 RPC 请求 JSON。
//   3. 以“紧凑 JSON + '\n'”的形式发送给服务端。
//   4. 接收服务端的确认响应并格式化打印。
//
// 编译环境：Linux / macOS（POSIX Socket）
// 编译命令：
//   g++ -std=c++11 -O2 -Wall -Wextra -pedantic rpc_client.cpp -o rpc_client
// =============================================================================

#include <nlohmann/json.hpp>

#include <cerrno>
#include <csignal>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

using json = nlohmann::json;

namespace {

// 防止响应异常巨大时客户端无限累积内存。
const std::size_t kMaxMessageBytes = 1024 * 1024;  // 1 MiB

void close_socket(int fd) {
    if (fd >= 0) {
        ::close(fd);
    }
}

// TCP 的 send 可能只送出部分数据，必须循环直到全部送出。
bool send_all(int fd, const std::string& data) {
    std::size_t sent = 0;

    while (sent < data.size()) {
        const ssize_t n = ::send(fd, data.data() + sent, data.size() - sent, 0);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }

        if (n == 0) {
            return false;
        }

        sent += static_cast<std::size_t>(n);
    }

    return true;
}

// 读取一行响应。服务端同样以 '\n' 作为消息结束标志。
bool receive_one_line(int fd, std::string& message) {
    message.clear();
    char buffer[4096];

    while (true) {
        const std::size_t newline_pos = message.find('\n');

        if (newline_pos != std::string::npos) {
            message.erase(newline_pos);
            if (!message.empty() && message.back() == '\r') {
                message.pop_back();
            }
            return true;
        }

        if (message.size() >= kMaxMessageBytes) {
            return false;
        }

        const ssize_t n = ::recv(fd, buffer, sizeof(buffer), 0);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }

        // 服务端关闭连接时，把已收到的内容视为完整响应。
        if (n == 0) {
            return !message.empty();
        }

        message.append(buffer, static_cast<std::size_t>(n));
    }
}

// 使用 getaddrinfo 连接服务端。相比直接把字符串转成 IPv4，
// getaddrinfo 可以同时支持 "127.0.0.1" 和 "localhost" 等主机名。
int connect_to_server(const std::string& host, const std::string& port) {
    addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;      // 允许 IPv4 或 IPv6
    hints.ai_socktype = SOCK_STREAM;  // TCP

    addrinfo* addresses = nullptr;
    const int lookup_result =
        ::getaddrinfo(host.c_str(), port.c_str(), &hints, &addresses);

    if (lookup_result != 0) {
        std::cerr << "解析服务端地址失败：" << ::gai_strerror(lookup_result)
                  << "\n";
        return -1;
    }

    int connected_fd = -1;

    // 一个主机名可能解析出多个地址，逐一尝试直到连接成功。
    for (addrinfo* current = addresses; current != nullptr;
         current = current->ai_next) {
        const int fd =
            ::socket(current->ai_family, current->ai_socktype,
                     current->ai_protocol);

        if (fd < 0) {
            continue;
        }

        if (::connect(fd, current->ai_addr, current->ai_addrlen) == 0) {
            connected_fd = fd;
            break;
        }

        close_socket(fd);
    }

    ::freeaddrinfo(addresses);
    return connected_fd;
}

// 建立测试请求。这个函数集中展示请求 JSON 应该包含哪些字段。
json make_test_request() {
    json request;
    request["jsonrpc"] = "2.0";       // 协议版本
    request["id"] = "req-1001";       // 请求 ID，用于匹配响应
    request["method"] = "math.add";   // 想调用的远程方法
    request["params"] = json::array({12, 30});  // 参数列表

    // meta 是选填的附加信息；这里仅用于帮助观察与调试。
    request["meta"]["client"] = "rpc_client.cpp";
    request["meta"]["note"] = "basic test request";

    return request;
}

}  // namespace

int main(int argc, char* argv[]) {
    // 若服务端提前关闭连接，send() 可能触发 SIGPIPE。
    // 忽略该信号后，程序会收到 send() 的错误返回值，便于正常处理。
    std::signal(SIGPIPE, SIG_IGN);

    // 用法：
    //   ./rpc_client
    //   ./rpc_client 127.0.0.1 8080
    //   ./rpc_client localhost 8080
    std::string host = "127.0.0.1";
    std::string port = "8080";

    if (argc >= 2) {
        host = argv[1];
    }
    if (argc >= 3) {
        port = argv[2];
    }
    if (argc > 3) {
        std::cerr << "用法：" << argv[0] << " [host] [port]\n";
        return 1;
    }

    const int socket_fd = connect_to_server(host, port);
    if (socket_fd < 0) {
        std::cerr << "连接 " << host << ":" << port << " 失败："
                  << std::strerror(errno) << "\n";
        return 1;
    }

    std::cout << "已连接到 RPC 服务端 " << host << ":" << port << "\n";

    const json request = make_test_request();

    // 终端中格式化显示，方便人阅读。
    std::cout << "\n准备发送的 RPC 请求（格式化显示）：\n"
              << request.dump(4) << "\n";

    // 真正上网线时使用 dump() 产生紧凑单行 JSON，再附加 '\n'。
    // 不能使用 dump(4)，因为 pretty-print 产生的换行会破坏消息边界。
    const std::string wire_message = request.dump() + "\n";

    if (!send_all(socket_fd, wire_message)) {
        std::cerr << "发送请求失败：" << std::strerror(errno) << "\n";
        close_socket(socket_fd);
        return 1;
    }

    std::cout << "\n请求已发送：\n" << wire_message;

    // 告诉内核“客户端不会再发送更多数据”，但仍可继续接收响应。
    ::shutdown(socket_fd, SHUT_WR);

    std::string response_text;
    if (!receive_one_line(socket_fd, response_text)) {
        std::cerr << "接收响应失败，或服务端没有返回响应。\n";
        close_socket(socket_fd);
        return 1;
    }

    try {
        const json response = json::parse(response_text);
        std::cout << "\n收到服务端响应（格式化显示）：\n"
                  << response.dump(4) << "\n";
    } catch (const std::exception& exception) {
        // 即使响应不是合法 JSON，也把原始内容打印出来，便于排查问题。
        std::cerr << "响应 JSON 解析失败：" << exception.what() << "\n";
        std::cerr << "原始响应：" << response_text << "\n";
        close_socket(socket_fd);
        return 1;
    }

    close_socket(socket_fd);
    return 0;
}
