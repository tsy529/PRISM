#pragma once

#include <any>
#include <string>
#include <thread>
#include <arpa/inet.h>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <unistd.h>
#include <algorithm>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <numeric>

#include "../Share/Share.h"
#include "../Table/Table.h"
#include "../Message/Message.h"
#include "../Statistic/Statistic.h"

class Node {
public:
    // 节点的唯一标识，确定地址表和sock表中的位置
    u_int id;
    std::string name;

    // 构造和析构
    Node(u_int id, const std::string& name);
    ~Node();

    // tcp相关函数
    void startlistening();
    static void connectToPeers(std::vector<Node*> nodes);
    void stop();
    void sendMessage(Node* node, const Message &message);
    void sendMessage(u_int to_id, const Message &message);
    void sendMessageAndWait(u_int to_id, const Message &message);
    void sendOK(u_int to_id);
    void recvMessage(int sock);
    virtual void handleMessage(const Message& message, const std::string& message_str);
    static int extractPort(const std::string &address);     // 辅助函数，提取地址中的port

    // 变量存取/检查/打印操作
    std::any& getVar(std::string var_name);
    void setVar(std::string var_name, std::any value);
    void deleteVar(std::string var_name);
    Index& getTableIndex(std::string table_name, std::vector<std::string> attrs);
    void setTableIndex(std::string table_name, Index value);    // 设置表的索引变量
    void deleteTableIndex(std::string table_name, std::vector<std::string> attrs);    // 删除表对应attrs的索引
    bool checkTableIndex(std::string table_name, std::vector<std::string> attrs);    // 检查表是否有对应attrs的索引
    void printVars();
    void printVarsInFile();
    static std::string extractVarName(const std::string& var_name);     // [x]或x，返回x
    static std::string notVarName(const std::string& var_name);         // 返回取非时的变量名，例如[x]取非为[!x]，!y取非为y      

    // 变量秘密共享操作
    // 将已存储的int类型var以share_type共享给owners
    void shareInt(std::string var_name, u_int share_type, std::vector<Node*> owners);
    // 将未存储的int类型秘密以share_type共享给owners
    void shareInt(std::string var_name, int secret, u_int share_type, std::vector<Node*> owners);
    // 将int类型var的秘密共享对发送给to
    void revealIntTo(std::string var_name, Node *to);
    // 将未存储的Table以share_types共享给owners
    void shareTable(std::string table_name, Table& table, std::vector<u_int> share_types, std::vector<Node*> owners);
    // 将已存储的Table以share_types共享给owners
    void shareTable(std::string table_name, std::vector<u_int> share_types, std::vector<Node*> owners);
    // 将已存储的Table，所有列都以share_type共享给owners
    void shareTable(std::string table_name, u_int share_type, std::vector<Node*> owners);
    // 将Table的秘密共享对发送给to
    void revealTableTo(std::string table_name, Node *to);
    // 将已存储的Vector以share_type共享给owners
    void shareVec(std::string vec_name, u_int share_type, std::vector<Node*> owners);
    // 将Vector的秘密共享对发送给to
    void revealVecTo(std::string vec_name, Node* to);

    // 日志打印相关函数
    void log(const std::string& log) const;
    void log_process(size_t current, size_t total) const;
    void warning(const std::string& warning) const;
    void error(const std::string& log) const;
    void logInFile(const std::string& log) const;
    void warningInFile(const std::string& log) const;
    void errorInFile(const std::string& log) const;
    void clearFile();
    
protected:
    // 总服务器数量
    static const int NODE_MAXIMUM_NUM = 5;

    // 默认地址表
    static const std::string DEFAULT_ADDR[NODE_MAXIMUM_NUM];

    // tcp相关的变量
    int listen_sock;
    struct sockaddr_in listen_addr;
    std::vector<int> socks;
    
    std::vector<std::thread> recv_threads;
    std::mutex handle_msg_mtx;      // 确保一次只处理一条消息，防止资源占用

    // 存储持有的变量
    std::unordered_map<std::string, std::any> var_store;

    // 临时存储需要reveal的变量ShareTuple
    std::unordered_map<std::string, std::any> reveal_buffer;

    // 等待回应标识，确保Share出内容已被接受
    bool wait_response[NODE_MAXIMUM_NUM] = {false};
    void wait(u_int id);
    void waitOK(u_int id);
    bool isWaiting();

    // 打印控制位
    bool enable_log = false;
};

class Command {
public:
    static const u_int RESPONSE_OK = 0;
    static const u_int RECEIVE_INT_SHARE = 2;
    static const u_int REVEAL_INT = 3;
    static const u_int RECEIVE_TABLE_SHARE = 4;
    static const u_int REVEAL_TABLE = 5;
    static const u_int SORT_VECTOR = 6;
    static const u_int RECEIVE_VEC_SHARE = 7;
    static const u_int PERM_VECTOR = 8;
    static const u_int RECEIVE_VECS_SHARE = 9;
    static const u_int COMPARE_NEIGHBOR_EQ = 10;
    static const u_int OR_VECTOR = 11;
    static const u_int COMPARE_NEIGHBOR_GT = 12;
    static const u_int REVEAL_VEC = 13;
    static const u_int RECEIVE_AND_SHARE = 14;
};