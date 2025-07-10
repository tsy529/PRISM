#pragma once

#include "Node.h"

class Server : public Node {
public:
    // 构造函数
    Server(u_int id, const std::string& name) : Node(id, name) {}

    // 复写handleMessage函数
    void handleMessage(const Message& message, const std::string& message_str);

    // 复写，增加Server*对象的传参，将已存储的Table，所有列都以share_type共享给owners
    void shareTable(std::string table_name, u_int share_type, std::vector<Server*> servers);

    void join(std::string table1_name, std::string table2_name, std::string result_name);

    void concateTable(std::string table1_name, std::string table2_name, std::string result_name);
};