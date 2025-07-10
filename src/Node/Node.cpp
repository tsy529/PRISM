#include "Node.h"
#include <mutex>
#include <netinet/tcp.h>

std::mutex socks_mutex;       // 保护socks的互斥锁
std::mutex var_mutex;
std::condition_variable var_cv;
std::mutex handle_mutex;
std::mutex wait_mutex;
std::condition_variable wait_cv;
std::mutex print_mutex;


const std::string Node::DEFAULT_ADDR[NODE_MAXIMUM_NUM] = {
    "127.0.0.1:5000",   // Server0
    "127.0.0.1:5001",   // Server1
    "127.0.0.1:5002",   // Server2
    "127.0.0.1:5003",   // DataOwner
    "127.0.0.1:5004"    // TrustedThird
};

Node::Node(u_int id, const std::string& name) {
    if (id < 0 || id >= NODE_MAXIMUM_NUM) {
        error("Invalid ID");
        exit(EXIT_FAILURE);
    }

    this->id = id;
    this->name = name;
    this->clearFile();

    listen_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock < 0) {
        error("Error creating listening socket");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    if (setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        error("Error setting socket options");
        exit(EXIT_FAILURE);
    }

    try {
        listen_addr.sin_family = AF_INET;
        listen_addr.sin_addr.s_addr = INADDR_ANY;
        listen_addr.sin_port = htons(extractPort(DEFAULT_ADDR[id]));

        if (bind(listen_sock, (struct sockaddr *)&listen_addr, sizeof(listen_addr)) < 0) {
            error("Error binding listening socket");
            close(listen_sock);
            exit(EXIT_FAILURE);
        }

        if (listen(listen_sock, NODE_MAXIMUM_NUM) < 0) {
            error("Error listening on socket");
            close(listen_sock);
            exit(EXIT_FAILURE);
        }

        // 预留用于连接的sock位置
        socks.resize(NODE_MAXIMUM_NUM, -1);
        log("starting to listen on address " + DEFAULT_ADDR[id]);
    } catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
    }
}

Node::~Node() {
    stop();
}

void Node::startlistening() {
    while (true) {
        struct sockaddr_in addr;
        socklen_t addrlen = sizeof(addr);
        int new_sock = accept(listen_sock, (struct sockaddr*)&addr, &addrlen);
        if (new_sock < 0) {
            if (errno == EBADF || errno == EINVAL) {
                // 如果是主动关闭
                break;
            } else {
                error("Error accepting connection");
                continue;
            }
        }

        socks.push_back(new_sock);
        recv_threads.emplace_back(&Node::recvMessage, this, new_sock);
    }
}

void Node::connectToPeers(std::vector<Node*> nodes) {
    // nodes之间相互建立TCP连接
    for (auto si = nodes.begin(); si != nodes.end(); si++) {
        for (auto sj = nodes.begin(); sj != nodes.end(); sj++) {
            if ((*si)->id == (*sj)->id) continue;
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) {
                throw std::runtime_error("Runtime error in connectToPeers(): Error creating socket");
            }
           
            try {
                struct sockaddr_in addr;    
                addr.sin_family = AF_INET;
                addr.sin_port = htons(extractPort(DEFAULT_ADDR[(*sj)->id]));
                addr.sin_addr.s_addr = inet_addr("127.0.0.1");

                while (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
                    // std::cout << DEFAULT_ADDR[(*si)->id] + " failed to connect " + DEFAULT_ADDR[(*sj)->id] + ", retrying..." << std::endl;
                    sleep(1);
                }

                (*si)->socks[(*sj)->id] = sock;
                // std::cout << DEFAULT_ADDR[(*si)->id] + " has connected to " + DEFAULT_ADDR[(*sj)->id] << std::endl;
            } catch (std::exception& e) {
                std::cerr << e.what() << std::endl;
            }
        }
    }
}

void Node::stop() {
    // std::lock_guard<std::mutex> lock(socks_mutex);
    // 关闭监听套接字
    if (listen_sock != -1) {
        shutdown(listen_sock, SHUT_RDWR);
        close(listen_sock);
        listen_sock = -1;
    }

    // 关闭所有连接套接字
    for (int sock : socks) {
        if (sock != -1) {
            shutdown(sock, SHUT_RDWR);
            close(sock);
            sock = -1;
        }
    }

    // 等待所有接受线程结束
    for (auto& th : recv_threads) {
        if (th.joinable()) {
            th.join();
        }
    }

    recv_threads.clear();
}

void Node::sendMessage(Node* node, const Message& message) {
    sendMessage(node->id, message);
}

void Node::sendMessage(u_int to_id, const Message &message) {
    std::string message_str = message.toString();       // 多态，自动调用各类message的toString()
    uint64_t message_len = htobe64(message_str.size());

    // 发送消息长度
    if (send(socks[to_id], &message_len, sizeof(message_len), 0) < 0) {
        error("Error sending message length to Server" + std::to_string(to_id));
    }

    // 发送消息内容
    if (send(socks[to_id], message_str.c_str(), message_str.size(), 0) < 0) {
        error("Error sending message content to Server" + std::to_string(to_id));
    }
    // log("send \"" + message_str + "\" to Server_ID: " + std::to_string(to_id));
}

// void Node::sendMessageAndWait(u_int to_id, const Message &message) {
//     sendMessage(to_id, message);
//     // 等待接收方回复
//     wait(to_id);
//     while (isWaiting()) {}
// }

void Node::sendMessageAndWait(u_int to_id, const Message &message) {
    wait(to_id);
    sendMessage(to_id, message);

    // 持续等待 OK 回复
    std::unique_lock<std::mutex> lock(wait_mutex);
    wait_cv.wait(lock, [this] { return !this->isWaiting(); });
}

void Node::recvMessage(int sock) {
    while (true) {
        uint64_t message_len;
        ssize_t total_head_received = 0;
        char* buffer = reinterpret_cast<char*>(&message_len);

        // 循环接收，保证完整读取8字节消息头
        while (total_head_received < sizeof(message_len)) {
            ssize_t len_recv = recv(sock, 
                                    buffer + total_head_received,                // 使用偏移地址
                                    sizeof(message_len) - total_head_received,   // 剩余待接收量
                                    0);
            if (len_recv == 0) {    // 对端正常关闭连接
                // log("Connection closed by peer");
                close(sock);
                return;
            } else if (len_recv < 0) {  // 发生系统错误
                error("Error receiving message length");
                close(sock);
                return;
            }
            total_head_received += len_recv;
        }

        message_len = be64toh(message_len);     // 大端字节序转换为主机字节序

        // 为防止恶意的超大长度值，可以添加合理性检查
        const uint64_t MAX_MESSAGE_SIZE = 1ULL << 50;  // 4GB上限，可根据需求调整
        if (message_len > MAX_MESSAGE_SIZE) {
            error("Message length too large: " + std::to_string(message_len));
            break;
        } else {
            // log("MAX_MESSAGE_SIZE is " + std::to_string(MAX_MESSAGE_SIZE) + ", and current message size is " + std::to_string(message_len));
        }

        // 分块接收消息
        const size_t CHUNK_SIZE = 1024 * 1024;
        std::string message_str;
        message_str.reserve(std::min(message_len, CHUNK_SIZE));
        uint64_t total_received = 0;
        std::vector<char> chunk_buffer(CHUNK_SIZE);
        while(total_received < message_len) {
            size_t remaining = message_len - total_received;
            size_t current_chunk_size = std::min(remaining, CHUNK_SIZE);
            ssize_t bytes_recv = recv(sock, chunk_buffer.data(), current_chunk_size, 0);
            if (bytes_recv > 0) {
                message_str.append(chunk_buffer.data(), bytes_recv);
                total_received += bytes_recv;
            } else if (bytes_recv == 0) {
                log("Connection closed by peer");
                return;
            } else {
                error("Error receiving message");
                return;
            }
        }
        
        if (total_received == message_len) {
            Message message(message_str);
            handleMessage(message, message_str);
        }
    }
}

void Node::sendOK(u_int to_id) {
    BscMessage response(this->id, to_id, Command::RESPONSE_OK);
    sendMessage(to_id, response);
}


void Node::handleMessage(const Message& message, const std::string& message_str) {
    std::lock_guard<std::mutex> guard(handle_msg_mtx);

    if (message.command == Command::RECEIVE_INT_SHARE) {
        VarMessage vmsg(message_str);
        SharePair sp(vmsg.var_str);
        this->setVar(vmsg.var_name, sp);
        this->sendOK(message.from);
    } 

    else if (message.command == Command::RESPONSE_OK) {
        waitOK(message.from);
        // log("receive OK from Server " + std::to_string(message.from));
    }

    else if (message.command == Command::REVEAL_INT) {
        VarMessage vmsg(message_str);
        std::string reveal_name = vmsg.var_name.substr(1, vmsg.var_name.length() - 2);
        try {
            int reveal;
            if (var_store.count(vmsg.var_name) > 0) {   
                // 如果本身有要重构的变量的一份SharePair，那么Reveal协议只会让S_{i-1}把他的SharePair发过来，使用x_{i-1}即可
                SharePair sp = std::any_cast<SharePair>(this->getVar(vmsg.var_name));   // 拿自己那份sp
                SharePair sp_received(vmsg.var_str);    // 提取vmsg中接受的sp
                // 按共享类型重构秘密值
                if (sp.type == ARITHMETIC_SHARING) {
                    int res = (sp[0] + sp[1] + sp_received[0]) % MOD;
                    reveal = res < 0 ? res + MOD : res; //  确保落在[0,MOD)
                } else if (sp.type == BINARY_SHARING) {
                    reveal = sp[0] ^ sp[1] ^ sp_received[0];
                }
                // 将重构后的值存入
                this->setVar(reveal_name, reveal);
            } else {
                // 如果本身没有要重构变量的SharePair，则使用reveal_buffer接受多份数据
                std::unique_lock<std::mutex> lock(handle_mutex);
                SharePair sp(vmsg.var_str);
                if (reveal_buffer.count(vmsg.var_name) <= 0) {    // 如果buffer中没有则初始化一个
                    reveal_buffer[vmsg.var_name] = ShareTuple(sp.type);
                }
                auto& st = std::any_cast<ShareTuple&>(reveal_buffer[vmsg.var_name]);
                st[vmsg.from] = sp[0];

                if (st.isFull()) {  // 如果填满了则reveal
                    reveal = st.compute();
                    // 删除buffer中的数据
                    reveal_buffer.erase(vmsg.var_name);
                    // 将重构后的值存入
                    this->setVar(reveal_name, reveal);
                }
            }
            this->sendOK(message.from);
        } catch (const std::bad_any_cast& e) {
            warning("Bad any cast in handleMessage(), command is REVEAL_INT: " + std::string(e.what()));
        }
    }

    else if (message.command == Command::RECEIVE_TABLE_SHARE) {
        VarMessage vmsg(message_str);
        Table sst(Table::SHARE_PAIR_MODE);
        sst.readFromString(vmsg.var_str);
        this->setVar(vmsg.var_name, sst);
        sendOK(message.from);
    }

    else if (message.command == Command::REVEAL_TABLE) {
        VarMessage vmsg(message_str);
        std::string reveal_name = vmsg.var_name.substr(1, vmsg.var_name.length() - 2);
        try {
            Table t_received(Table::SHARE_PAIR_MODE);
            t_received.readFromString(vmsg.var_str);
            Table reveal_table(Table::INT_MODE);
            reveal_table.headers = t_received.headers;
            reveal_table.max_freqs = t_received.max_freqs;
            for (int i = 0; i < reveal_table.headers.size(); i++) {
                reveal_table.share_types.push_back(NO_SHARING);
            }

            if (var_store.count(vmsg.var_name) > 0) {
                // 如果本身有一个mode为SharePair的Table
                Table t = std::any_cast<Table>(this->getVar(vmsg.var_name));
                for (int i = 0; i < t.data.size(); i++) {
                    // 计算Data
                    std::vector<std::any> reveal_row;
                    for (int j = 0; j < t.headers.size(); j++) {
                        SharePair sp = std::any_cast<SharePair>(t.data[i][j]);
                        SharePair sp_received = std::any_cast<SharePair>(t_received.data[i][j]);
                        ShareTuple st({sp[0], sp[1], sp_received[0]}, t.share_types[j]);
                        reveal_row.push_back(st.compute());
                    }
                    reveal_table.data.push_back(reveal_row);

                    // 计算isNull
                    SharePair sp = std::any_cast<SharePair>(t.is_null[i]);
                    SharePair sp_received = std::any_cast<SharePair>(t_received.is_null[i]);
                    ShareTuple st({sp[0], sp[1], sp_received[0]}, BINARY_SHARING);
                    reveal_table.is_null.push_back(st.compute());
                }
                // 将重构后的值存入
                this->setVar(reveal_name, reveal_table);
            } else {
                // 如果本身没有要重构变量的SharePair，则使用reveal_buffer接受多份数据
                std::unique_lock<std::mutex> lock(handle_mutex);
                if (reveal_buffer.count(reveal_name) <= 0) {    // 如果buffer中没有则初始化一个
                    reveal_buffer[reveal_name] = std::vector<Table>();
                }
                auto& ts = std::any_cast<std::vector<Table>&>(reveal_buffer[reveal_name]);
                ts.push_back(t_received);
                // 如果都以赋值
                if (ts.size() == SHARE_PARTY_NUM) {
                    for (int i = 0; i < t_received.data.size(); i++) {
                        // 计算Data
                        std::vector<std::any> reveal_row;
                        for (int j = 0; j < t_received.headers.size(); j++) {
                            SharePair sp0 = std::any_cast<SharePair>(ts[0].data[i][j]);
                            SharePair sp1 = std::any_cast<SharePair>(ts[1].data[i][j]);
                            SharePair sp2 = std::any_cast<SharePair>(ts[2].data[i][j]);
                            ShareTuple st({sp0[0], sp1[0], sp2[0]}, t_received.share_types[j]);
                            reveal_row.push_back(st.compute());
                            //这里有问题
                        }
                        reveal_table.data.push_back(reveal_row);

                        // 计算isNull
                        SharePair sp0 = std::any_cast<SharePair>(ts[0].is_null[i]);
                        SharePair sp1 = std::any_cast<SharePair>(ts[1].is_null[i]);
                        SharePair sp2 = std::any_cast<SharePair>(ts[2].is_null[i]);
                        ShareTuple st({sp0[0], sp1[0], sp2[0]}, BINARY_SHARING);
                        reveal_table.is_null.push_back(st.compute());
                    }
                    // 将重构后的值存入
                    this->setVar(reveal_name, reveal_table);
                    // 删除缓存中对应的内容
                    this->reveal_buffer.erase(reveal_name);
                }
            }
            this->sendOK(message.from);
        } catch (const std::bad_any_cast& e) {
            warning("Bad any cast in handleMessage(), command is REVEAL_TABLE: " + std::string(e.what()));
        }
    } 
    
    else if (message.command == Command::RECEIVE_VEC_SHARE) {
        VarMessage vmsg(message_str);
        std::vector<SharePair> sps = String2SPs(vmsg.var_str);
        this->setVar(vmsg.var_name, sps);
        this->sendOK(message.from);
    } 
    
    else if (message.command == Command::RECEIVE_VECS_SHARE) {
        VarsMessage vsmsg(message_str);
        for (const auto& var : vsmsg.vars) {
            std::vector<SharePair> sps = String2SPs(var.second);
            this->setVar(var.first, sps);
        }
        this->sendOK(vsmsg.from);
    }

    else if (message.command == Command::REVEAL_VEC) {
        VarMessage vmsg(message_str);
        std::string reveal_name = extractVarName(vmsg.var_name);
        try {
            std::vector<int> reveal;
            if (var_store.count(vmsg.var_name) > 0) {
                // 如果本身有要重构的变量的一份SharePair，那么Reveal协议只会让S_{i-1}把他的SharePair发过来，使用x_{i-1}即可
                std::vector<SharePair> sps = std::any_cast<std::vector<SharePair>>(this->getVar(vmsg.var_name));
                auto sps_received = String2SPs(vmsg.var_str);
                // 按共享类型重构
                if (sps[0].type == ARITHMETIC_SHARING) {
                    for (int i = 0; i < sps.size(); i++) {
                        int res = (sps[i][0] + sps[i][1] + sps_received[i][0]) % MOD;
                        reveal.push_back(res < 0 ? res + MOD : res);    //  确保落在[0,MOD)
                    }
                } else if (sps[0].type == BINARY_SHARING) {
                    for (int i = 0; i < sps.size(); i++) {
                        reveal.push_back(sps[i][0] ^ sps[i][1] ^ sps_received[i][0]);
                    }
                }
                // 将重构后的值存入
                this->setVar(reveal_name, reveal);
            } else {
                // 如果本身没有要重构变量的SharePair，则使用reveal_buffer接受多份数据
                std::unique_lock<std::mutex> lock(handle_mutex);
                auto sps = String2SPs(vmsg.var_str);
                if (reveal_buffer.count(vmsg.var_name) <= 0) {
                    reveal_buffer[vmsg.var_name] = std::vector<ShareTuple>(sps.size(), ShareTuple(sps[0].type));
                }
                auto& sts = std::any_cast<std::vector<ShareTuple>&>(reveal_buffer[vmsg.var_name]);
                for (int i = 0; i < sts.size(); i++) {
                    sts[i][vmsg.from] = sps[i][0];
                }

                if (sts[0].isFull()) {
                    for (int i = 0; i < sts.size(); i++) {
                        reveal.push_back(sts[i].compute());
                    }
                    reveal_buffer.erase(vmsg.var_name);
                    this->setVar(reveal_name, reveal);
                }
            }
            this->sendOK(message.from);
        } catch (const std::bad_any_cast& e) {
            warning("Bad any cast in handleMessage(), command is REVEAL_VEC: " + std::string(e.what()));
        }
    }

    else if (message.command == Command::RECEIVE_AND_SHARE) {
        log("Do Command::RECEIVE_AND_SHARE");
        VarMessage vmsg(message_str);
        auto zi = std::any_cast<int>(this->getVar(vmsg.var_name + "_i"));
        auto zip1 = std::stoi(vmsg.var_str);
        this->setVar(vmsg.var_name, SharePair({zi, zip1}, BINARY_SHARING));
        this->deleteVar(vmsg.var_name + "_i");
        log("Finish Command::RECEIVE_AND_SHARE");
        sendOK(message.from);
    }
}

// std::any& Node::getVar(std::string var_name) {
//     // 确保该变量已经在var_store中保存
//     while (var_store.find(var_name) == var_store.end()) {}
//     return var_store[var_name];
// }

std::any& Node::getVar(std::string var_name) {
    std::unique_lock<std::mutex> lock(var_mutex);
    // 等待直到 var_store 中存在 var_name
    var_cv.wait(lock, [&](){ return var_store.find(var_name) != var_store.end(); });

    return var_store[var_name];
}

// void Node::setVar(std::string var_name, std::any value) {
//     log("Saving " + var_name);
//     if (value.type() == typeid(int) || value.type() == typeid(SharePair) || value.type() == typeid(Table) ||
//         value.type() == typeid(std::vector<Index>) || value.type() == typeid(std::vector<SharePair>) ||
//         value.type() == typeid(std::vector<int>)) {
//         var_store[var_name] = value;
//         // 如果是存入一个Table类型的变量，则自动创建该Table的索引容器
//         if (value.type() == typeid(Table)) {
//             // 用不带括号的变量名+“-idx”表示索引变量
//             std::string table_idx_name = extractVarName(var_name) + "-idx";
//             // 如果本没有该Table的索引变量
//             if (var_store.find(table_idx_name) == var_store.end()) {
//                 // 存储索引变量
//                 var_store[table_idx_name] = std::vector<Index>();
//             }
//         }
//     } else {
//         error("Invalid argument in setVar(): wrong type");
//         exit(EXIT_FAILURE);
//     }
// }

void Node::setVar(std::string var_name, std::any value) {
    log("Saving " + var_name);
    if (value.type() == typeid(int) || value.type() == typeid(SharePair) || value.type() == typeid(Table) ||
        value.type() == typeid(std::vector<Index>) || value.type() == typeid(std::vector<SharePair>) ||
        value.type() == typeid(std::vector<int>)) {
        // 对var_store进行加锁，确保线程安全地修改变量
        std::lock_guard<std::mutex> lock(var_mutex);
        var_store[var_name] = value;
        // 如果是存入一个Table类型的变量，则自动创建该Table的索引容器
        if (value.type() == typeid(Table)) {
            // 用不带括号的变量名+“-idx”表示索引变量
            std::string table_idx_name = extractVarName(var_name) + "-idx";
            // 如果本没有该Table的索引变量
            if (var_store.find(table_idx_name) == var_store.end()) {
                // 存储索引变量
                var_store[table_idx_name] = std::vector<Index>();
            }
        }
        // 通知所有可能在等待该变量的线程
        var_cv.notify_all();
        log("Finished setting variable: " + var_name);
    } else {
        error("Invalid argument in setVar(): wrong type");
        exit(EXIT_FAILURE);
    }
}

// void Node::deleteVar(std::string var_name) {
//     log("Deleting " + var_name);
//     auto& value = getVar(var_name);
//     // 如果是Table类型变量，则需要级联删除他的索引变量
//     if (value.type() == typeid(Table)) {
//         var_store.erase(extractVarName(var_name) + "-idx");
//     }
//     var_store.erase(var_name);
//     log("Finish Delete " + var_name);
// }

void Node::deleteVar(std::string var_name) {
    log("Deleting " + var_name);
    {
        std::lock_guard<std::mutex> lock(var_mutex);
        if (var_store.find(var_name) == var_store.end()) {
            error("Variable " + var_name + " not found; skipping delete.");
            return;
        }
        // 如果是Table类型变量，则需要级联删除他的索引变量
        auto& value = var_store[var_name];
        if (value.type() == typeid(Table)) {
            var_store.erase(extractVarName(var_name) + "-idx");
        }
        var_store.erase(var_name);
    }
    log("Finish Delete " + var_name);
}

Index& Node::getTableIndex(std::string table_name, std::vector<std::string> attrs) {
    std::string table_idx_name = extractVarName(table_name) + "-idx";
    auto& index_arr = std::any_cast<std::vector<Index>&>(this->getVar(table_idx_name));

    // 先为attrs排序
    std::sort(attrs.begin(), attrs.end());
    for (auto itr = index_arr.begin(); itr != index_arr.end(); itr++) {
        if (itr->getAttrs().size() != attrs.size()) {
            continue;
        }
        auto copy = itr->getAttrs();
        std::sort(copy.begin(), copy.end());
        if (copy == attrs) {
            return *itr;
        }
    }

    // 如果找不到则报错
    error("Invalid argument in getTableIndex(): no such index");
    exit(EXIT_FAILURE);
}

void Node::setTableIndex(std::string table_name, Index value) {
    std::string table_idx_name = extractVarName(table_name) + "-idx";
    auto& index_arr = std::any_cast<std::vector<Index>&>(this->getVar(table_idx_name));
    index_arr.push_back(value);
}

void Node::deleteTableIndex(std::string table_name, std::vector<std::string> attrs) {
    std::string table_idx_name = extractVarName(table_name) + "-idx";
    auto& index_arr = std::any_cast<std::vector<Index>&>(this->getVar(table_idx_name));

    // 先为attrs排序
    std::sort(attrs.begin(), attrs.end());
    for (auto itr = index_arr.begin(); itr != index_arr.end(); itr++) {
        if (itr->getAttrs().size() != attrs.size()) {
            continue;
        }
        auto copy = itr->getAttrs();
        std::sort(copy.begin(), copy.end());
        if (copy == attrs) {
            index_arr.erase(itr);
            break;
        }
    }
}

bool Node::checkTableIndex(std::string table_name, std::vector<std::string> attrs) {
    std::string table_idx_name = extractVarName(table_name) + "-idx";
    auto& index_arr = std::any_cast<std::vector<Index>&>(this->getVar(table_idx_name));

    // 先为attrs排序
    std::sort(attrs.begin(), attrs.end());
    for (auto itr = index_arr.begin(); itr != index_arr.end(); itr++) {
        if (itr->getAttrs().size() != attrs.size()) {
            continue;
        }
        auto copy = itr->getAttrs();
        std::sort(copy.begin(), copy.end());
        if (copy == attrs) {
            return true;
        }
    }

    return false;
}

void Node::printVars() {
    std::string var_list = "\nname\ttype\tvalue\n";
    for (const auto& [key, value] : var_store) {
        var_list += key + "\t";
        try {
            if (value.type() == typeid(int)) {    // 如果是明文(int)
                var_list += "p\t";
                var_list += std::to_string(std::any_cast<int>(value));
            } else if (value.type() == typeid(SharePair)) {    // 如果是共享值（Shared）
                const SharePair& var = std::any_cast<const SharePair&>(value);
                var_list += var.type == ARITHMETIC_SHARING ? "as\t" : "bs\t";
                var_list += "(" + std::to_string(var[0]) + "," + std::to_string(var[1]) + ")";
            } else if (value.type() == typeid(Table)) {     // 如果是表（Table）
                const Table& var = std::any_cast<const Table&>(value);
                if (var.mode == Table::INT_MODE) { 
                    var_list += "p\t";
                } else {
                    var_list += "s\t";
                }
                var_list += "Table_below\n";
                var_list += "\033[34m" + var.toString() + "\033[0m";
                std::replace(var_list.begin(), var_list.end(), ' ', '\t');
            } else if (value.type() == typeid(std::vector<Index>)) {
                var_list += "p\tIndex_below\n";
                auto& index_arr = std::any_cast<const std::vector<Index>&>(value);
                for (const auto& index : index_arr) {
                    var_list += index.toString();
                }
            } else {
                warning("Unknow type var: " + key);
            }
            var_list += "\n";
        } catch (const std::bad_any_cast& e) {
            warning("Bad any cast in printVars: " + std::string(e.what()));
        }
    }
    log(var_list);
}

void Node::printVarsInFile() {
    log("Printing vars to file");
    std::string var_list = "\nname\ttype\tvalue\n";
    for (const auto& [key, value] : var_store) {
        var_list += key + "\t";
        try {
            if (value.type() == typeid(int)) {    // 如果是明文(int)
                var_list += "p\t";
                var_list += std::to_string(std::any_cast<int>(value));
            } else if (value.type() == typeid(SharePair)) {    // 如果是共享值（Shared）
                const SharePair& var = std::any_cast<const SharePair&>(value);
                var_list += var.type == ARITHMETIC_SHARING ? "as\t" : "bs\t";
                var_list += "(" + std::to_string(var[0]) + "," + std::to_string(var[1]) + ")";
            } else if (value.type() == typeid(Table)) {     // 如果是表（Table）
                const Table& var = std::any_cast<const Table&>(value);
                if (var.mode == Table::INT_MODE) { 
                    var_list += "p\t";
                } else {
                    var_list += "s\t";
                }
                var_list += "Table_below\n";
                var_list += var.toString();
                std::replace(var_list.begin(), var_list.end(), ' ', '\t');
            } else if (value.type() == typeid(std::vector<Index>)) {
                var_list += "p\tIndex_below\n";
                auto& index_arr = std::any_cast<const std::vector<Index>&>(value);
                for (const auto& index : index_arr) {
                    var_list += index.toString();
                }
            } else {
                warning("Unknow type var: " + key);
            }
            var_list += "\n";
        } catch (const std::bad_any_cast& e) {
            warning("Bad any cast in printVars: " + std::string(e.what()));
        }
    }
    logInFile(var_list);
    log("Print vars to file successfully");
}

std::string Node::extractVarName(const std::string& var_name) {
    // 提取不带括号的var_name
    std::string extract_name;
    if (var_name[0] == '[' && var_name.back() == ']') {
        extract_name = var_name.substr(1, var_name.length() - 2); 
    } else {
        extract_name = var_name;
    }
    return extract_name;
}

std::string Node::notVarName(const std::string& var_name) {
    if (var_name.size() >= 2 && var_name.substr(0, 2) == "[!") { // 如果以 "[!" 开头，则去掉 "!"，即保留 "[" 后面的部分
        return "[" + var_name.substr(2);
    } else if (var_name[0] == '!') { // 如果以 "!" 开头，则去掉最前面的 "!"
        return var_name.substr(1);
    } else if (var_name[0] == '[') { // 如果以 "[" 开头，则在 "[" 后面添加 "!"
        return "[!" + var_name.substr(1);
    } else { // 其他情况下，直接在最前面加上 "!"
        return "!" + var_name;
    }
}

void Node::shareInt(std::string var_name, u_int share_type, std::vector<Node*> owners) {
    log("Sharing int " + var_name + " to " + std::accumulate(std::next(owners.begin()), owners.end(), owners[0]->name, [](const std::string &acc, Node* ptr) {
        return acc + ", " + ptr->name;
    }));
    if (var_store[var_name].type() != typeid(int)) {
        error("Error sharing type in shareInt()");
        exit(EXIT_FAILURE);
    }

    try {
        int secret = std::any_cast<int>(var_store[var_name]);

        int s[3];
        if (share_type == ARITHMETIC_SHARING) {
            s[0] = std::rand() % MOD;
            s[1] = std::rand() % MOD;
            s[2] = (secret - s[0] - s[1]) % MOD;
        }

        else {
            s[0] = std::rand() % MOD;
            s[1] = std::rand() % MOD;
            s[2] = (secret ^ s[0] ^ s[1]) % MOD;
        }

        for (u_int i = 0; i < SHARE_PARTY_NUM; i++) {
            SharePair sp({s[i], s[(i + 1) % SHARE_PARTY_NUM]}, share_type);
            if (owners[i]->id != id) { // 发送份额(s_i,s_{i+1})给S_i，此处的i为Server在owners中的索引
                VarMessage message(id, owners[i]->id, Command::RECEIVE_INT_SHARE, "[" + var_name + "]", sp.toString());
                sendMessage(owners[i], message);
                wait(owners[i]->id);    // 等待回复标志
            } else { // 保存自己的份额
                var_store["[" + var_name + "]"] = sp;
            }
        }

        // 等待所有Server收到并处理完消息
        while (isWaiting()) {}  
    } catch (const std::bad_any_cast& e) {
        warning("Bad any cast in shareInt(): " + std::string(e.what()));
    }
}

void Node::shareInt(std::string var_name, int secret, u_int share_type, std::vector<Node*> owners) {
    // 保存新变量
    this->setVar(var_name, secret);

    log("Sharing int" + var_name + " to " + std::accumulate(std::next(owners.begin()), owners.end(), owners[0]->name, [](const std::string &acc, Node* ptr) {
        return acc + ", " + ptr->name;
    }));
    int s[3];
    if (share_type == ARITHMETIC_SHARING) {
        s[0] = std::rand() % MOD;
        s[1] = std::rand() % MOD;
        s[2] = (secret - s[0] - s[1]) % MOD;
    } else {
        s[0] = std::rand() % MOD;
        s[1] = std::rand() % MOD;
        s[2] = (secret ^ s[0] ^ s[1]) % MOD;
    }

    for (u_int i = 0; i < SHARE_PARTY_NUM; i++) {
        SharePair sp({s[i], s[(i + 1) % SHARE_PARTY_NUM]}, share_type);
        if (owners[i]->id != id) {      // 发送份额(s_i,s_{i+1})给S_i，此处的i为Server在owners中的索引
            VarMessage message(id, owners[i]->id, Command::RECEIVE_INT_SHARE, "[" + var_name + "]", sp.toString());
            sendMessage(owners[i], message);
            wait(owners[i]->id);    // 等待回复标志
        } else {  // 保存自己的份额
            var_store["[" + var_name + "]"] = sp;
        }
    }

    // 等待所有Server收到并处理完消息
    while (isWaiting()) {}  
}

void Node::revealIntTo(std::string var_name, Node *to) {
    log("Revealing int " + var_name + " to " + to->name);
    try {
        SharePair sp = std::any_cast<SharePair>(this->getVar(var_name));
        VarMessage vmsg(this->id, to->id, Command::REVEAL_INT, var_name, sp.toString());
        this->sendMessageAndWait(to->id, vmsg);
    } catch (const std::bad_any_cast& e) {
        warning("Bad any cast in revealIntTo(): " + std::string(e.what()));
    }
}

void Node::shareTable(std::string table_name, Table& t, std::vector<u_int> share_types, std::vector<Node*> owners) {
    log("Sharing table " + table_name + " to " + std::accumulate(std::next(owners.begin()), owners.end(), owners[0]->name, [](const std::string &acc, Node* ptr) {
        return acc + ", " + ptr->name;
    }));
    try {
        if (t.mode != Table::INT_MODE) {
            error("Error table data type in shareTable()");
            exit(EXIT_FAILURE);
        }

        if (t.headers.size() != share_types.size()) {
            error("Error table share type in shareTable()");
            exit(EXIT_FAILURE);
        }
        
        // 为表t生成ShareTuple版本
        Table st(Table::SHARE_TUPLE_MODE);
        st.headers = t.headers;
        st.share_types = share_types;
        st.max_freqs = t.max_freqs;
        for (int i = 0; i < t.data.size(); i++) {
            // 生成Data的ShareTuple
            std::vector<std::any> share_row;
            for (int j = 0; j < t.data[i].size(); j++) {
                int value = std::any_cast<int>(t.data[i][j]); 
                share_row.push_back(ShareTuple(value, share_types[j]));
            }
            st.data.push_back(share_row);
            // 生成isNull的ShareTuple
            int value = std::any_cast<int>(t.is_null[i]);
            st.is_null.push_back(ShareTuple(value, BINARY_SHARING));
        }

        // 从ShareTuple的表st中提取发给各方的SharePair版本
        for (u_int i = 0; i < 3; i++) {
            Table sst(Table::SHARE_PAIR_MODE);
            sst.headers = t.headers;
            sst.share_types = share_types;
            sst.max_freqs = t.max_freqs;
            for (int j = 0; j < st.data.size(); j++) {
                // 共享Data
                std::vector<std::any> ss_row;
                for (int k = 0; k < st.data[j].size(); k++) {
                    auto share_value = std::any_cast<ShareTuple>(st.data[j][k]);
                    SharePair ss({share_value[i], share_value[(i + 1) % 3]}, share_types[k]);
                    ss_row.push_back(ss);
                }
                sst.data.push_back(ss_row);

                // 共享isNull
                auto share_value = std::any_cast<ShareTuple>(st.is_null[j]);
                SharePair ss({share_value[i], share_value[(i + 1) % 3]}, BINARY_SHARING);
                sst.is_null.push_back(ss);
            }

            if (owners[i]->id != this->id) {
                VarMessage message(this->id, owners[i]->id, Command::RECEIVE_TABLE_SHARE, "[" + table_name + "]", sst.toString());
                sendMessage(owners[i]->id, message);
                wait(owners[i]->id);
            } else {
                var_store["[" + table_name + "]"] = sst;
            }
        }

        // 等待所有Server收到并处理完消息
        while (isWaiting()) {}
    } catch (const std::bad_any_cast& e) {
        warning("Bad any cast in shareTable(): " + std::string(e.what()));
    }
}

void Node::shareTable(std::string table_name, std::vector<u_int> share_types, std::vector<Node*> owners) {
    try {
        Table& t = std::any_cast<Table&>(var_store[table_name]);
        shareTable(table_name, t, share_types, owners);
    } catch (const std::bad_any_cast& e) {
        warning("Bad any cast in shareTable(): " + std::string(e.what()));
    }
}

void Node::shareTable(std::string table_name, u_int share_type, std::vector<Node*> owners) {
    try {
        Table& t = std::any_cast<Table&>(var_store[table_name]);
        shareTable(table_name, t, std::vector<u_int>(t.headers.size(), share_type), owners);
    } catch (const std::bad_any_cast& e) {
        warning("Bad any cast in shareTable(): " + std::string(e.what()));
    }
}

void Node::revealTableTo(std::string table_name, Node *to) {
    log("Revealing table " + table_name + " to " + to->name);
    try{
        // 发送表
        Table t = std::any_cast<Table>(this->getVar(table_name));
        VarMessage vmsg_table(this->id, to->id, Command::REVEAL_TABLE, table_name, t.toString());
        this->sendMessageAndWait(to->id, vmsg_table);
    } catch (const std::bad_any_cast& e) {
        warning("Bad any cast in revealTableTo(): " + std::string(e.what()));
    }
}

void Node::revealVecTo(std::string vec_name, Node* to) {
    log("Revealing vector " + vec_name + " to " + to->name);
    try {
        std::vector<SharePair> sps = std::any_cast<std::vector<SharePair>>(this->getVar(vec_name));
        VarMessage vmsg(this->id, to->id, Command::REVEAL_VEC, vec_name, SPs2String(sps));
        this->sendMessageAndWait(to->id, vmsg);
    } catch (const std::bad_any_cast& e) {
        warning("Bad any cast in revealVecTo(): " + std::string(e.what()));
    }
}

void Node::log(const std::string& log) const {
    if (enable_log) {
        std::lock_guard<std::mutex> guard(print_mutex);
        std::cout << "<LOG>[" + this->name + "]: " << log << std::endl;
    }
}

void Node::log_process(size_t current, size_t total) const {
    if (!enable_log) { return; }

    if (total == 0) return;

    // 计算当前百分比（取整）
    int currentPrecentage = static_cast<int>((current * 100.0) / total);

    // 静态变量用于记录上一次输出的百分比，保证在同一个循环中同一百分比只打印一次
    static int lastPrintedPercentage = -1;

    if (currentPrecentage != lastPrintedPercentage) {
        std::lock_guard<std::mutex> guard(print_mutex);
        lastPrintedPercentage = currentPrecentage;
        std::cout << "\r" << "<LOG>[" + this->name + "]: " << currentPrecentage << "% complete (total: " << total << ")" << std::flush;   
    }

    if (current == total) {
        std::cout << std::endl;
    }
}

void Node::warning(const std::string& warning) const {
    if (enable_log) {
        std::lock_guard<std::mutex> guard(print_mutex);
        // 设置warning内容为蓝色
        std::cout << "\033[34m" << "<WARNING>[" + this->name + "]: " << warning << "\033[0m" << std::endl;
    }
}

void Node::error(const std::string& error) const {
    if (enable_log) {
        std::lock_guard<std::mutex> guard(print_mutex);
        // 设置error内容为红色
        std::cerr << "\033[31m" << "<ERROR>:[" + this->name + "] " << error << "\033[0m" << std::endl;
    }
    
}

void Node::logInFile(const std::string& log) const {
    std::ofstream outFile(this->name + "_log.txt", std::ios::app);
    if (!outFile.is_open()) {
        error("Unable to open log file");
        return;
    }
    outFile << log << std::endl;
    outFile.close();
}

void Node::clearFile() {
    std::ofstream outFile(this->name + "_log.txt", std::ios::trunc); // 使用trunc模式打开并清空文件
    if (!outFile.is_open()) {
        error("Unable to open log file for clearing");
        return;
    }
    outFile.close();
}

int Node::extractPort(const std::string &address) {
    std::string::size_type pos = address.find(':');
    if (pos == std::string::npos) {
        throw std::invalid_argument("Invalid argument in extractPort(): Invalid address format");
        exit(EXIT_FAILURE);
    }
    return std::stoi(address.substr(pos + 1));
}

void Node::wait(u_int id) {
    {
        std::lock_guard<std::mutex> lock(wait_mutex);
        wait_response[id] = true;
    }
}

void Node::waitOK(u_int id) {
    {
        std::lock_guard<std::mutex> lock(wait_mutex);
        wait_response[id] = false;
    }
    wait_cv.notify_all();
}

bool Node::isWaiting() {
    // 检查是不是所有项都是false，即都没有在等待回应
    for (bool f : wait_response) {
        if (f) return true;
    }
    return false;
}
