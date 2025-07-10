#pragma once

#include "Node.h"
#include "Server.h"

class ThirdParty : public Node {
private: 
    // 声明需要作为哪三个服务器组的可信第三方
    std::vector<Node*> servers;

    // 参数预存区
    std::vector<std::vector<std::any>> args_buffer;

    // 指令预存区
    std::vector<u_int> cmd_buffer;

    std::mutex thirdparty_handle_msg_mutx;

public:
    // 构造函数
    ThirdParty(u_int id, const std::string& name, std::vector<Node*> servers) : Node(id, name), servers(servers), args_buffer(servers.size()), cmd_buffer(servers.size(), 0) {}

    // 复写handleMessage函数
    void handleMessage(const Message& message, const std::string& message_str);

    // 参数和指令缓存区的操作
    bool checkCmd();
    std::any executeCmd();
    void clearBuffer();

    // 变量秘密共享操作
    // 复写，增加Server*对象的传参，将已存储的Table，所有列都以share_type共享给owners
    void shareInt(std::string var_name, u_int share_type, std::vector<Server*> servers);
    // 复写，增加Server*对象的传参，将已存储的Table，所有列都以share_type共享给owners
    void shareTable(std::string table_name, u_int share_type, std::vector<Server*> servers);
    // 要求重构int类型的var
    void revealInt(std::string var_name);
    // 要求重构Table
    void revealTable(std::string table_name);
    // 要求重构Vector
    void revealVec(std::string vec_name);

    // 提供的第三方功能
    // Join: R ⋈ S = T，default |T| = |R|*|S| for security
    void join(std::string r_name, std::string s_name, std::string t_name, const uint64_t& padding_to = CARTESIAN_BOUND);
    // BucketJoin 
    void bucketJoin(std::string r_name, std::string s_name, std::string t_name, const uint64_t& padding_to = CARTESIAN_BOUND);
    // 检查匹配结果
    void checkMatch(std::string table_name, std::vector<std::vector<int>> symmetries);
    // 检查匹配Injectivity
    void checkInjectivity(std::string table_name);
    // 检查匹配Symmetry
    void checkSymmetry(std::string table_name, std::vector<std::vector<int>> symmetries);
    // Sort
    void sort(std::string vec_name, std::string result_name);
    // 检查vec中是否存在相邻元素相同, 若有则返回1，若无返回0
    void hasNeighborEqual(std::string vec_name, std::string result_name);
    // 检查vec是否升序排列, 若是则返回0，若不是返回1
    void isOrdered(std::string vec_name, std::string result_name);

    void printVec(const std::vector<int>& vec);

};