#pragma once

#include "../Node/Server.h"
#include "../Node/ThirdParty.h"
#include <future>
#include <mutex>

class Protocol {
public:
    std::vector<Server*> servers;
    ThirdParty* third_party;

    bool use_third_party = true;   // 对于部分功能，是否使用可信第三方，默认为true

    std::mutex doEach_mutex;

    // --------------------- 构造函数 ---------------------
    Protocol(std::vector<Server*> servers, ThirdParty* third_party) : servers(servers), third_party(third_party) {}

    // --------------------- 秘密共享和重构 ---------------------
    // server申请重构int类型var
    void revealInt(std::string var_name, Node *node);
    // server申请重构Table类型var
    void revealTable(std::string table_name, Node *node);

    // --------------------- 基础功能协议 ---------------------
    // [y] ← ![x]
    void logicNOT(std::string x_name, std::string y_name = "");
    // [z] ← [x] ∧ [y]
    void logicAND(std::string x_name, std::string y_name, std::string z_name);
    // [z] ← [x] ∨ [y]
    void logicOR(std::string x_name, std::string y_name, std::string z_name);

    // --------------------- 特殊功能协议 ---------------------
    // 有Join Plan，星型分解和对称节点
    void subgraphMatchingOnlyMPC(const Table& query_graph, const std::string& result_name);
    // 无Join Plan，默认左深连接，仅使用MF Bound
    void subgraphMatchingNoJoinPlan(const Table& query_graph, const std::string& result_name);
    // 有Join Plan，星型分解和对称节点
    void subgraphMatching(const Table& query_graph, const std::string& result_name);
    // 安全约束验证协议
    void checkMatch(std::string table_name, std::vector<std::vector<int>> symmetries);
    // 安全约束验证协议（新）
    void checkMatch_new(std::string table_name, std::vector<std::vector<int>> symmetries);

    // --------------------- 打印函数 ---------------------
    void log(const std::string& log) const;
    void log_process(size_t current, size_t total) const;

private:
    bool isLog = true;

    template <typename Func> 
    void doEach(Func func) {
        for (auto server : servers) {
            func(server);
        }
    }

    template<typename Func>
    void doEachAsync(Func func) {
        std::lock_guard<std::mutex> guard(doEach_mutex);
        std::vector<std::future<void>> futures;
        // 打印开始提示（主线程）
        // log("Start doEachAsync");

        // 为每个Server启动异步任务
        for (auto server : servers) {
            futures.emplace_back(std::async(std::launch::async, [server, &func] {
                // server->log("Start Action");
                func(server);
                // server->log("End Action");
            }));
        }

        // 阻塞等待所有异步任务完成
        for (auto& future : futures) {
            future.get();
        }

        // log("End doEachAsync");
    }

    std::chrono::steady_clock::time_point start_, end_;     // 时间点
    // 获取执行时间（默认单位为毫秒）
    template <typename Duration = std::chrono::milliseconds> long long getDuration() const {
        return std::chrono::duration_cast<Duration>(end_ - start_).count();
    }

    // 使用初始边表匹配结果计算边表为edges的子图的AGM上界，初始边表匹配结果的size为size
    uint64_t computeAGMBound(const std::vector<std::vector<int>>& edges, uint64_t size);

    // 计算stars的最优连接次序
    std::vector<int> optimizeJoinOrder(const std::vector<Utils::StarGraph>& stars, uint64_t size);
};