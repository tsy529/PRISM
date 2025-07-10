#include "ThirdParty.h"
#include <unordered_set> 
#include <algorithm>

void ThirdParty::handleMessage(const Message& message, const std::string& message_str) {
    std::lock_guard<std::mutex> guard(thirdparty_handle_msg_mutx);
    if (message.command == Command::SORT_VECTOR) {
        // (SORT, [v])
        VarMessage vmsg(message_str);
        // 设置vmsg.from在缓存区中对应的变量和指令
        auto arg = String2SPs(vmsg.var_str);
        args_buffer[vmsg.from].push_back(arg);
        cmd_buffer[message.from] = Command::SORT_VECTOR;

        // 检查是否收集其各方的变量和指令
        if (checkCmd()) {
            // 执行指令，提取结果
            // log("Begin SORT_VECTOR");
            std::any result = executeCmd();
            auto perm_sts = std::any_cast<std::vector<ShareTuple>>(result);
            // 将结果共享给各方
            for (int i = 0; i < servers.size(); i++) {
                std::vector<SharePair> sps;
                for (int j = 0; j < perm_sts.size(); j++) {
                    sps.push_back(SharePair({perm_sts[j][i], perm_sts[j][(i + 1) % servers.size()]}, perm_sts[j].type));
                }
                VarMessage back_vmsg(this->id, servers[i]->id, Command::RECEIVE_VEC_SHARE, vmsg.result_name, SPs2String(sps));
                sendMessage(servers[i]->id, back_vmsg);
                sendOK(servers[i]->id);
            }
            clearBuffer();
            // log("Complete SORT_VECTOR");
        }
    } else if (message.command == Command::PERM_VECTOR) {
        // (PERM, [pi], {[v1], [v2], ...})
        VarsMessage vsmsg(message_str);
        // 设置vmsg.from在缓存区中对应的变量和指令
        // 提取perm
        auto perm = String2SPs(vsmsg.vars[0].second);
        args_buffer[vsmsg.from].push_back(perm);
        // 提取待排序的向量组vecs
        std::vector<std::vector<SharePair>> vecs;
        for (int i = 1; i < vsmsg.vars.size(); i++) {
            vecs.push_back(String2SPs(vsmsg.vars[i].second));
        }
        args_buffer[vsmsg.from].push_back(vecs);
        cmd_buffer[message.from] = Command::PERM_VECTOR;

        // 检查是否收集各方的变量和指令
        if (checkCmd()) {
            // 执行指令，提取结果
            // log("Begin PERM_VECTOR");
            std::any result = executeCmd();
            auto permed_vecs_st = std::any_cast<std::vector<std::vector<ShareTuple>>>(result);
            // 将结果共享给各方
            for (int i = 0; i < servers.size(); i++) {
                std::vector<std::pair<std::string, std::string>> vars;
                for (int j = 0; j < permed_vecs_st.size(); j++) {
                    std::vector<SharePair> sps;
                    for (const auto& st : permed_vecs_st[j]) {
                        sps.push_back(SharePair({st[i], st[(i + 1) % servers.size()]}, st.type));
                    }
                    std::string var_name = "[" + extractVarName(vsmsg.vars[j + 1].first) + "']";
                    vars.push_back({var_name, SPs2String(sps)}); // vsmsg.vars[j]是perm，索引需要+1才是vecs的起始
                }
                VarsMessage back_vsmsg(this->id, servers[i]->id, Command::RECEIVE_VECS_SHARE, vars, vsmsg.result_name);
                sendMessage(servers[i]->id, back_vsmsg);
                sendOK(servers[i]->id);
            }
            clearBuffer();
            // log("Complete PERM_VECTOR");
        }
    } else if (message.command == Command::COMPARE_NEIGHBOR_EQ) {
        // (CPN_EQU, [v])
        VarMessage vmsg(message_str);
        // 设置vmsg.from在缓存区中对应的变量和指令
        auto arg = String2SPs(vmsg.var_str);
        args_buffer[vmsg.from].push_back(arg);
        cmd_buffer[vmsg.from] = Command::COMPARE_NEIGHBOR_EQ;

        // 检查是否收集齐各方的变量和指令
        if (checkCmd()) {
            // 执行指令，提取结果
            // log("Begin COMPARE_NEIGHBOR_EQ");
            std::any result = executeCmd();
            auto flags_st = std::any_cast<std::vector<ShareTuple>>(result);
            // 将结果共享给各方
            for (int i = 0; i < SHARE_PARTY_NUM; i++) {
                std::vector<SharePair> sps;
                for (int j = 0; j < flags_st.size(); j++) {
                    sps.push_back(SharePair({flags_st[j][i], flags_st[j][(i + 1) % SHARE_PARTY_NUM]}, flags_st[j].type));
                }
                VarMessage back_vmsg(this->id, servers[i]->id, Command::RECEIVE_VEC_SHARE, vmsg.result_name, SPs2String(sps));
                sendMessage(servers[i]->id, back_vmsg);
                sendOK(servers[i]->id);
            }
            clearBuffer();
            // log("Complete COMPARE_NEIGHBOR_EQ");
        }
    } else if (message.command == Command::COMPARE_NEIGHBOR_GT) {
        // (CPN_ASC, [v])
        VarMessage vmsg(message_str);
        // 设置vmsg.from在缓存区中对应的变量和指令
        auto arg = String2SPs(vmsg.var_str);
        args_buffer[vmsg.from].push_back(arg);
        cmd_buffer[vmsg.from] = Command::COMPARE_NEIGHBOR_GT;

        // 检查是否收集齐各方的变量和指令
        if (checkCmd()) {
            // 执行指令，提取结果
            // log("Begin COMPARE_NEIGHBOR_GT");
            std::any result = executeCmd();
            auto flags_st = std::any_cast<std::vector<ShareTuple>>(result);
            // 将结果共享给各方
            for (int i = 0; i < SHARE_PARTY_NUM; i++) {
                std::vector<SharePair> sps;
                for (int j = 0; j < flags_st.size(); j++) {
                    sps.push_back(SharePair({flags_st[j][i], flags_st[j][(i + 1) % SHARE_PARTY_NUM]}, flags_st[j].type));
                }
                VarMessage back_vmsg(this->id, servers[i]->id, Command::RECEIVE_VEC_SHARE, vmsg.result_name, SPs2String(sps));
                sendMessage(servers[i]->id, back_vmsg);
                sendOK(servers[i]->id);
            }
            clearBuffer();
            // log("Complete COMPARE_NEIGHBOR_GT");
        }
    } else if (message.command == Command::OR_VECTOR) {
        // (ORV, [v])
        VarMessage vmsg(message_str);
        // 设置vmsg.from在缓存区中对应的变量和指令
        auto arg = String2SPs(vmsg.var_str);
        args_buffer[vmsg.from].push_back(arg);
        cmd_buffer[vmsg.from] = Command::OR_VECTOR;

        // 检查是否收集齐各方的变量和指令
        if (checkCmd()) {
            // 执行指令，提取结果
            // log("Begin OR_VECTOR");
            std::any result = executeCmd();
            auto d_st = std::any_cast<ShareTuple>(result);
            // 将结果共享给各方
            for (int i = 0; i < SHARE_PARTY_NUM; i++) {
                SharePair sp({d_st[i], d_st[(i + 1) % SHARE_PARTY_NUM]}, d_st.type);
                VarMessage back_vmsg(this->id, servers[i]->id, Command::RECEIVE_INT_SHARE, vmsg.result_name, sp.toString());
                sendMessage(servers[i]->id, back_vmsg);
                sendOK(servers[i]->id);
            }
            clearBuffer();
            // log("Complete OR_VECTOR");
        }
    }
    else {  // 自动调用Node的handleMessage函数
        Node::handleMessage(message, message_str);
    }
}

bool ThirdParty::checkCmd() {
    if (cmd_buffer.size() != servers.size()) {
        return false;
    }

    // 检查是否有三个相同的指令
    u_int first_cmd = cmd_buffer[0];
    for (auto cmd : cmd_buffer) {
        if (cmd != first_cmd) {
            return false;
        }
    }

    return true;
}

std::any ThirdParty::executeCmd() {
    u_int cmd = cmd_buffer[0];
    // 检查各方给的参数数量是否一致
    size_t first_size = args_buffer[0].size();
    for (int i = 0; i < args_buffer.size(); i++) {
        if (args_buffer[i].size() != first_size) {
            error("Runtime Error in ThirdParty::executeCmd(): Inconsistent number of parameters");
            throw std::runtime_error("Runtime Error in ThirdParty::executeCmd(): Inconsistent number of parameters");
        }
    }

    if (cmd == Command::SORT_VECTOR) {
        // 输入一组数据，排序后输出映射
        // 重构向量
        std::vector<std::vector<SharePair>> multi_server_sps;
        
        // 提取各方的SharePair向量，秘密共享的向量放在args_buffer[server_idx]的第一个
        for (int i = 0; i < args_buffer.size(); i++) {
            multi_server_sps.push_back(std::any_cast<std::vector<SharePair>&>(args_buffer[i][0]));
        }
        // 用各方SharePair的首个份额，构造ShareTuple向量
        std::vector<ShareTuple> sts;
        u_int share_type = multi_server_sps[0][0].type;
        sts.reserve(multi_server_sps[0].size());
        for (int i = 0; i < multi_server_sps[0].size(); i++) {
            sts.push_back(ShareTuple({multi_server_sps[0][i][0], multi_server_sps[1][i][0], multi_server_sps[2][i][0]}, share_type));
        }
        // 计算明文下的vec
        std::vector<int> vec(sts.size());
        for (int i = 0; i < sts.size(); i++) {
            vec[i] = sts[i].compute();
        }
        // 计算perm
        auto perm = Utils::getSortingPermutation(vec);
        // log("perm:");
        // printVec(perm);

        // 将结果转为ShareTuple数组
        std::vector<ShareTuple> perm_sts;
        perm_sts.reserve(perm.size());
        for (int i = 0; i < perm.size(); i++) {
            perm_sts.push_back(ShareTuple(perm[i], share_type));
        }

        return perm_sts;
    } else if (cmd == Command::PERM_VECTOR) {
        // 重构perm和vecs
        std::vector<std::vector<SharePair>> multi_server_perm;
        std::vector<std::vector<std::vector<SharePair>>> multi_server_vecs;
        for (int i = 0; i < args_buffer.size(); i++) {
            multi_server_perm.push_back(std::any_cast<std::vector<SharePair>&>(args_buffer[i][0]));
            multi_server_vecs.push_back(std::any_cast<std::vector<std::vector<SharePair>>&>(args_buffer[i][1]));
        }
        size_t vec_size = multi_server_perm[0].size();  // perm和vecs中的向量长度都是相等的，用vec_size表示
        std::vector<ShareTuple> perm_st;
        std::vector<std::vector<ShareTuple>> vecs_st;

        // 将各方的perm转化为ShareTuple数组
        perm_st.reserve(vec_size);
        for (int i = 0; i < vec_size; i++) {
            perm_st.push_back(ShareTuple({multi_server_perm[0][i][0], multi_server_perm[1][i][0], multi_server_perm[2][i][0]},
                                            multi_server_perm[0][i].type));
        }
        // 将各方的vecs转化为ShareTuple数组
        vecs_st.reserve(multi_server_vecs[0].size());
        for (int i = 0; i < multi_server_vecs[0].size(); i++) {
            std::vector<ShareTuple> vec_st;
            vec_st.reserve(vec_size);
            for (int j = 0; j < vec_size; j++) {
                vec_st.push_back(ShareTuple({multi_server_vecs[0][i][j][0], multi_server_vecs[1][i][j][0], multi_server_vecs[2][i][j][0]},
                                            multi_server_vecs[0][i][j].type));
            }
            vecs_st.push_back(vec_st);
        }

        // 计算明文下的perm和vecs
        std::vector<int> perm;
        perm.reserve(vec_size);
        for (int i = 0; i < vec_size; i++) {
            perm.push_back(perm_st[i].compute());
        }

        std::vector<std::vector<int>> vecs;
        vecs.reserve(vecs_st.size());
        for (int i = 0; i < vecs_st.size(); i++) {
            std::vector<int> vec;
            vec.reserve(vec_size);
            for (int j = 0; j < vec_size; j++) {
                vec.push_back(vecs_st[i][j].compute());
            }
            vecs.push_back(vec);
        }

        // 计算按照perm排序后的vecs
        // log("permed vecs:");
        std::vector<std::vector<int>> permed_vecs;
        for (auto vec : vecs) {
            permed_vecs.push_back(Utils::applyPermutation(perm, vec));
            // printVec(permed_vecs.back());
        }

        // 将结果转变为ShareTuple数组
        std::vector<std::vector<ShareTuple>> permed_vecs_st;
        permed_vecs_st.reserve(permed_vecs.size());
        for (int i = 0; i < permed_vecs.size(); i++) {
            std::vector<ShareTuple> permed_vec_st;
            permed_vec_st.reserve(vec_size);
            u_int share_type = multi_server_vecs[0][i][0].type;
            for (int j = 0; j < vec_size; j++) {
                permed_vec_st.push_back(ShareTuple(permed_vecs[i][j], share_type));
            }
            permed_vecs_st.push_back(permed_vec_st);
        }

        return permed_vecs_st;
    } else if (cmd == Command::COMPARE_NEIGHBOR_EQ) {
        // 输入一组数据，输出邻居间比较的标志位数组
        // 重构向量
        std::vector<std::vector<SharePair>> multi_server_sps;

        // 提取各方的SharePair向量，秘密共享的向量放在args_buffer[server_idx]的第一个
        for (int i = 0; i < args_buffer.size(); i++) {
            multi_server_sps.push_back(std::any_cast<std::vector<SharePair>&>(args_buffer[i][0]));
        }
        // 用各方SharePair的首个份额，构造ShareTuple向量
        std::vector<ShareTuple> sts;
        u_int share_type = multi_server_sps[0][0].type;
        sts.reserve(multi_server_sps[0].size());
        for (int i = 0; i < multi_server_sps[0].size(); i++) {
            sts.push_back(ShareTuple({multi_server_sps[0][i][0], multi_server_sps[1][i][0], multi_server_sps[2][i][0]}, share_type));
        }
        // 计算明文下的vec
        std::vector<int> vec(sts.size());
        for (int i = 0; i < sts.size(); i++) {
            vec[i] = sts[i].compute();
        }

        // 相邻比较，生成标志位数组
        std::vector<int> flags(vec.size(), 0);
        for (int i = 1; i < vec.size(); i++) {
            if (vec[i] == vec[i - 1]) {
                flags[i] = 1;
            }
        }
        // log("flags:");
        // printVec(flags);

        // 将结果转为ShareTuple数组
        std::vector<ShareTuple> flags_st;
        flags_st.reserve(flags.size());
        for (int i = 0; i < flags.size(); i++) {
            flags_st.push_back(ShareTuple(flags[i], share_type));
        }

        return flags_st;
    } else if (cmd == Command::COMPARE_NEIGHBOR_GT) {
        // 输入一组数据，输出邻居间比较的标志位数组
        // 重构向量
        std::vector<std::vector<SharePair>> multi_server_sps;

        // 提取各方的SharePair向量，秘密共享的向量放在args_buffer[server_idx]的第一个
        for (int i = 0; i < args_buffer.size(); i++) {
            multi_server_sps.push_back(std::any_cast<std::vector<SharePair>&>(args_buffer[i][0]));
        }
        // 用各方SharePair的首个份额，构造ShareTuple向量
        std::vector<ShareTuple> sts;
        u_int share_type = multi_server_sps[0][0].type;
        sts.reserve(multi_server_sps[0].size());
        for (int i = 0; i < multi_server_sps[0].size(); i++) {
            sts.push_back(ShareTuple({multi_server_sps[0][i][0], multi_server_sps[1][i][0], multi_server_sps[2][i][0]}, share_type));
        }
        // 计算明文下的vec
        std::vector<int> vec(sts.size());
        for (int i = 0; i < sts.size(); i++) {
            vec[i] = sts[i].compute();
        }

        // 相邻比较，生成标志位数组
        std::vector<int> flags(vec.size(), 0);
        for (int i = 1; i < vec.size(); i++) {
            if (vec[i - 1] > vec[i]) {      // 相邻大于比较
                flags[i] = 1;
            }
        }
        // log("flags:");
        // printVec(flags);

        // 将结果转为ShareTuple数组
        std::vector<ShareTuple> flags_st;
        flags_st.reserve(flags.size());
        for (int i = 0; i < flags.size(); i++) {
            flags_st.push_back(ShareTuple(flags[i], share_type));
        }

        return flags_st;
    } else if (cmd == Command::OR_VECTOR) {
        // 输入一组数据，输出累积或
        // 重构向量
        std::vector<std::vector<SharePair>> multi_server_sps;

        // 提取各方的SharePair向量，秘密共享的向量放在args_buffer[server_idx]的第一个
        for (int i = 0; i < args_buffer.size(); i++) {
            multi_server_sps.push_back(std::any_cast<std::vector<SharePair>&>(args_buffer[i][0]));
        }
        // 用各方SharePair的首个份额，构造ShareTuple向量
        std::vector<ShareTuple> sts;
        u_int share_type = multi_server_sps[0][0].type;
        sts.reserve(multi_server_sps[0].size());
        for (int i = 0; i < multi_server_sps[0].size(); i++) {
            sts.push_back(ShareTuple({multi_server_sps[0][i][0], multi_server_sps[1][i][0], multi_server_sps[2][i][0]}, share_type));
        }
        // 计算明文下的vec
        std::vector<int> vec(sts.size());
        for (int i = 0; i < sts.size(); i++) {
            vec[i] = sts[i].compute();
        }

        // 计算累积或
        for (int num : vec) {
            if (num == 1) {
                // log("OR: 1");
                return ShareTuple(1, share_type);
            }
        }
        // log("OR: 0");
        return ShareTuple(0, share_type);

    } else {
        return std::any();
    }
}

void ThirdParty::clearBuffer() {
    for (int i = 0; i < servers.size(); i++) {
        args_buffer[i].clear();
        cmd_buffer[i] = 0;
    }
}

void ThirdParty::shareInt(std::string var_name, u_int share_type, std::vector<Server*> servers) {
    // 将Server*隐式转换为Node*
    std::vector<Node*> owners;
    owners.reserve(servers.size());
    for (auto server : servers) {
        owners.push_back(server);
    }

    Node::shareInt(var_name, share_type, owners);
}

void ThirdParty::shareTable(std::string table_name, u_int share_type, std::vector<Server*> servers) {
    // 将Server*隐式转换为Node*
    std::vector<Node*> owners;
    owners.reserve(servers.size());
    for (auto server : servers) {
        owners.push_back(server);
    }

    Node::shareTable(table_name, share_type, owners);
}


void ThirdParty::revealInt(std::string var_name) {
    auto it = std::find(std::begin(servers), std::end(servers), this);
    // 要求servers提供秘密共享份额
    for (int i = 0; i < SHARE_PARTY_NUM; i++) {
        servers[i]->revealIntTo(var_name, this);
    }
}

void ThirdParty::revealTable(std::string table_name) {
    // 要求servers提供秘密共享份额
    for (int i = 0; i < SHARE_PARTY_NUM; i++) {
        servers[i]->revealTableTo(table_name, this);
    }
}

void ThirdParty::revealVec(std::string vec_name) {
    // 要求servers提供秘密共享份额
    for (int i = 0; i < SHARE_PARTY_NUM; i++) {
        servers[i]->revealVecTo(vec_name, this);
    }
}

void ThirdParty::join(std::string r_name, std::string s_name, std::string t_name, const uint64_t& padding_to) {
    // 从servers接收r和s的SharePair表进行重构
    // log("Reveal input tables " + r_name + ", " + s_name + " for join");
    // this->revealTable(r_name);
    // this->revealTable(s_name);

    try {
        // 计算Join结果
        auto& r = std::any_cast<Table&>(this->getVar(extractVarName(r_name)));
        auto& s = std::any_cast<Table&>(this->getVar(extractVarName(s_name)));
        log("Join: " + t_name + " = " + r_name + " ⋈ " + s_name);
        Table t = Join(r, s, padding_to);
        log("Join finish!");

        // 统一以Binary sharing的方式共享表的数据
        // log("Join finish, sharing " + t_name + "...");
        // this->shareTable(extractVarName(t_name), t, std::vector<u_int>(t.headers.size(), BINARY_SHARING), servers);

        // 存储结果
        this->setVar(extractVarName(t_name), t);
    } catch (const std::bad_any_cast &e) {
        warning("Bad any cast in function ThirdParty::join(): " + std::string(e.what()) + " in " + r_name + " ⋈ " + s_name);
    }

    if (r_name != t_name) {
        this->deleteVar(extractVarName(r_name));
    }
    if (s_name != t_name) {
        this->deleteVar(extractVarName(s_name));
    }   
}

void ThirdParty::bucketJoin(std::string r_name, std::string s_name, std::string t_name, const uint64_t& padding_to) {
    // 从servers接收r和s的SharePair表进行重构
    // log("Reveal input tables " + r_name + ", " + s_name + " for bucketJoin");
    // this->revealTable(r_name);
    // this->revealTable(s_name);
    // 取值
    auto& r = std::any_cast<Table&>(this->getVar(extractVarName(r_name)));
    auto& s = std::any_cast<Table&>(this->getVar(extractVarName(s_name)));

    // 查找两个表中相同的headers
    std::vector<std::string> commonHeaders = getJoinKey(r, s);

    if (commonHeaders.empty()) {
        error("RunTime error in ThirdParty::bucketJoin(): No common headers found between tables");
        throw std::runtime_error("RunTime error in ThirdParty::bucketJoin(): No common headers found between tables");
    }

    // 验证是否有commonHeaders的索引
    bool has_index = true;
    for(auto& server : servers) {
        has_index &= server->checkTableIndex(r_name, commonHeaders);
        has_index &= server->checkTableIndex(s_name, commonHeaders);
    }

    if (!has_index) {
        // 如果没有连接键的索引，则无法进行bucketJoin，改为执行普通的Join
        log("Join: " + t_name + " = " + r_name + " ⋈ " + s_name);
        Table t = Join(r, s, padding_to);
        log("Join finish!");

        // 统一以Binary sharing的方式共享表的数据
        // log("Join finish, sharing " + t_name + "...");
        // this->shareTable(extractVarName(t_name), t, std::vector<u_int>(t.headers.size(), BINARY_SHARING), servers);

        // 存储结果
        this->setVar(extractVarName(t_name), t);
    } else {
        log("BucketJoin: " + t_name + " = " + r_name + " ⋈ " + s_name);

        // 如果有连接键的索引，则进行bucketJoin
        // 取出索引
        Index& r_idx = servers[0]->getTableIndex(r_name, commonHeaders);
        Index& s_idx = servers[0]->getTableIndex(s_name, commonHeaders);
        // 让表按照索引排序
        r.sortBy(commonHeaders);
        s.sortBy(commonHeaders);

        // 计算BucketJoin，默认填充到笛卡尔积大小
        auto result = BucketJoin(r, s, r_idx, s_idx, padding_to);
        log("Join finish!");

        // 存储结果
        this->setVar(extractVarName(t_name), result.first);
        this->setTableIndex(extractVarName(t_name), result.second);
    }

    if (r_name != t_name) {
        this->deleteVar(extractVarName(r_name));
    }
    if (s_name != t_name) {
        this->deleteVar(extractVarName(s_name));
    }  
}

void ThirdParty::checkMatch(std::string table_name, std::vector<std::vector<int>> symmetries) {
    // 从servers接收table的SharePair表进行重构
    // log("Reveal input table " + table_name + " for checkMatch");
    // this->revealTable(table_name);
    try {
        log("CheckMatch: " + table_name);
        auto& t = std::any_cast<Table&>(this->getVar(extractVarName(table_name)));

        for (int i = 0; i < t.data.size(); i++) {
            bool isValidRow = true;
            const auto& row = t.data[i];

            // 先检查此行本身是否有效
            auto value = std::any_cast<int>(t.is_null[i]);
            if (value == 1) {
                isValidRow = false;
            }

            // 检查对称约束
            if (isValidRow) {
                for (const auto &symmetry : symmetries) {
                    // 先找出对称组中存在在该行的元素
                    std::vector<int> indices;
                    for (auto& elem : symmetry) {
                        int idx = t.getColumnIdx(std::to_string(elem));
                        if (idx != -1) {
                            indices.push_back(idx);
                        }
                    }
                    // 逐个比较相邻的元素是否成偏序（升序）关系
                    for (int i = 0; i < indices.size() - 1; i++) {
                        int val1 = std::any_cast<int>(row[indices[i]]);
                        int val2 = std::any_cast<int>(row[indices[i + 1]]);
                        if (val1 >= val2) {
                            isValidRow = false;
                            break;
                        }
                    }

                    if (!isValidRow)
                        break;
                }
            }
            
            // 检查重复约束
            if (isValidRow) {
                std::vector<int> temp;
                temp.reserve(row.size());
                for (const auto& element : row) {
                    temp.push_back(std::any_cast<int>(element));
                }
                std::sort(temp.begin(), temp.end());
                for (int i = 0; i < temp.size(); i++) {
                    if (temp[i] == temp[i + 1]) {
                        isValidRow = false;
                        break;
                    }
                }
            }

            // 如果不满足则设置该行的isNull为1
            if (!isValidRow) {
                t.is_null[i] = 1;
            }

            log_process(i + 1, t.data.size());
        }
        log("CheckMatch finish!");

        // 统一以Binary sharing的方式共享表的数据
        // log("CheckMatch finish, sharing " + table_name + "...");
        // this->shareTable(table_name.substr(1, table_name.length() - 2), t, std::vector<u_int>(t.headers.size(), BINARY_SHARING), servers);

        // 存储结果
        this->setVar(extractVarName(table_name), t);
    } catch (const std::bad_any_cast &e) {
        warning("Bad any cast in function ThirdParty::checkMatch(): " + std::string(e.what()));
    }
    
    // 没有中间量，输出即输入，所以不删除
    // log("Delelte input table " + table_name);
    // this->deleteVar(extractVarName(table_name));
}

void ThirdParty::checkInjectivity(std::string table_name) {
    try {
        log("Check Injectivity: " + table_name);
        auto& t = std::any_cast<Table&>(this->getVar(extractVarName(table_name)));

        for (int i = 0; i < t.data.size(); i++) {
            bool isValidRow = true;
            const auto& row = t.data[i];

            // 先检查此行本身是否有效
            auto value = std::any_cast<int>(t.is_null[i]);
            if (value == 1) {
                isValidRow = false;
            }

            // 检查重复约束
            if (isValidRow) {
                std::vector<int> temp;
                temp.reserve(row.size());
                for (const auto& element : row) {
                    temp.push_back(std::any_cast<int>(element));
                }
                std::sort(temp.begin(), temp.end());
                for (int i = 0; i < temp.size(); i++) {
                    if (temp[i] == temp[i + 1]) {
                        isValidRow = false;
                        break;
                    }
                }
            }

            // 如果不满足则设置该行的isNull为1
            if (!isValidRow) {
                t.is_null[i] = 1;
            }

            log_process(i + 1, t.data.size());
        }
        log("Check Injectivity finish!");

        // 存储结果
        this->setVar(extractVarName(table_name), t);
    } catch (const std::bad_any_cast &e) {
        warning("Bad any cast in function ThirdParty::checkInjectivity(): " + std::string(e.what()));
    }
}

void ThirdParty::checkSymmetry(std::string table_name, std::vector<std::vector<int>> symmetries) {
    try {
        log("Check Symmetry: " + table_name);
        auto& t = std::any_cast<Table&>(this->getVar(extractVarName(table_name)));

        for (int i = 0; i < t.data.size(); i++) {
            bool isValidRow = true;
            const auto& row = t.data[i];

            // 先检查此行本身是否有效
            auto value = std::any_cast<int>(t.is_null[i]);
            if (value == 1) {
                isValidRow = false;
            }

            // 检查对称约束
            if (isValidRow) {
                for (const auto &symmetry : symmetries) {
                    // 先找出对称组中存在在该行的元素
                    std::vector<int> indices;
                    for (auto& elem : symmetry) {
                        int idx = t.getColumnIdx(std::to_string(elem));
                        if (idx != -1) {
                            indices.push_back(idx);
                        }
                    }
                    // 逐个比较相邻的元素是否成偏序（升序）关系
                    for (int i = 0; i < indices.size() - 1; i++) {
                        int val1 = std::any_cast<int>(row[indices[i]]);
                        int val2 = std::any_cast<int>(row[indices[i + 1]]);
                        if (val1 >= val2) {
                            isValidRow = false;
                            break;
                        }
                    }

                    if (!isValidRow)
                        break;
                }
            }
            
            // 如果不满足则设置该行的isNull为1
            if (!isValidRow) {
                t.is_null[i] = 1;
            }

            log_process(i + 1, t.data.size());
        }
        log("Check Symmetry finish!");

        // 存储结果
        this->setVar(extractVarName(table_name), t);
    } catch (const std::bad_any_cast &e) {
        warning("Bad any cast in function ThirdParty::checkSymmetry(): " + std::string(e.what()));
    }
}

void ThirdParty::sort(std::string vec_name, std::string result_name) {
    try {
        auto vec = std::any_cast<std::vector<int>>(this->getVar(extractVarName(vec_name)));
        // log("Sorting vec " + vec_name);
        std::sort(vec.begin(), vec.end());
        this->setVar(extractVarName(result_name), vec);
    } catch (const std::bad_any_cast &e) {
        warning("Bad any cast in function ThirdParty::sort(): " + std::string(e.what()));
    }
}

void ThirdParty::hasNeighborEqual(std::string vec_name, std::string result_name) {
    try {
        auto& vec = std::any_cast<std::vector<int>&>(this->getVar(extractVarName(vec_name)));
        int flag = 0;
        for (int i = 1; i < vec.size(); i++) {
            if (vec[i - 1] == vec[i]) {
                flag = 1;
                break;
            }
        }
        this->setVar(extractVarName(result_name), flag);
    } catch (const std::bad_any_cast &e) {
        warning("Bad any cast in function ThirdParty::hasNeighborEqual(): " + std::string(e.what()));
    }
}

void ThirdParty::isOrdered(std::string vec_name, std::string result_name) {
    try {
        auto& vec = std::any_cast<std::vector<int>&>(this->getVar(extractVarName(vec_name)));
        int flag = 0;
        for (int i = 1; i < vec.size(); i++) {
            if (vec[i - 1] >= vec[i]) {
                flag = 1;
                break;
            }
        }
        this->setVar(extractVarName(result_name), flag);
    } catch (const std::bad_any_cast &e) {
        warning("Bad any cast in function ThirdParty::isOrdered(): " + std::string(e.what()));
    }
}

void ThirdParty::printVec(const std::vector<int>& vec) {
    std::string output = "[";
    for (auto& elem : vec) {
        output += std::to_string(elem);
        output += " ";
    }
    output += "]";
    log(output);
}