#include "Server.h"

void Server::handleMessage(const Message& message, const std::string& message_str) {
    if (message.command == 100) { 
        
    } 

    else {  // 自动调用Node的handleMessage函数
        Node::handleMessage(message, message_str);
    }
}

void Server::shareTable(std::string table_name, u_int share_type, std::vector<Server*> servers) {
    // 将Server*隐式转换为Node*
    std::vector<Node*> owners;
    owners.reserve(servers.size());
    for (auto server : servers) {
        owners.push_back(server);
    }

    Node::shareTable(table_name, share_type, owners);
}

void Server::join(std::string table1_name, std::string table2_name, std::string result_name) {
    // [K] := key([X]) // key([Y])
    Table& table1 = std::any_cast<Table&>(this->getVar(table1_name));
    Table& table2 = std::any_cast<Table&>(this->getVar(table2_name));
    std::vector<std::string> join_key = getJoinKey(table1, table2);
    auto key_columns = table1.select(join_key);
    auto kc2 = table2.select(join_key);
    key_columns.reserve(key_columns.size() + kc2.size());
    key_columns.insert(key_columns.end(), kc2.begin(), kc2.end());

    // [N] := isNull([X]) // isNull([Y])
    
}

void Server::concateTable(std::string table1_name, std::string table2_name, std::string result_name) {
    auto& table1 = std::any_cast<Table&>(this->getVar(table1_name));
    auto& table2 = std::any_cast<Table&>(this->getVar(table2_name));
    Table result(table1);
    result.data.insert(result.data.end(), table2.data.begin(), table2.data.end());
    setVar(result_name, result);
}