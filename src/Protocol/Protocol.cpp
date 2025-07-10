#include "Protocol.h"

uint64_t AGM_BOUND_ = CARTESIAN_BOUND;
uint64_t MIX_BOUND_ = CARTESIAN_BOUND;

std::mutex ptl_print_mutex;

void Protocol::revealInt(std::string var_name, Node *node) {
    auto it = std::find(std::begin(servers), std::end(servers), node);
    if (it != std::end(servers)) {
        // 如果node本身持有份额，则让node的前一个Server发送SharePair 
        auto prevIt = (it == std::begin(servers) ? std::prev(std::end(servers)) : std::prev(it));
        (*prevIt)->revealIntTo(var_name, node);
    } else {
        // 如过node本身没有份额，则各server将SharePair发送给node
         for (int i = 0; i < SHARE_PARTY_NUM; i++) {
            servers[i]->revealIntTo(var_name, node);
        }
    }
}

void Protocol::revealTable(std::string table_name, Node *node) {
    auto it = std::find(std::begin(servers), std::end(servers), node);
    if (it != std::end(servers)) {
        // 如果node本身持有份额，则让node的前一个Server发送SharePair 
        auto prevIt = (it == std::begin(servers) ? std::prev(std::end(servers)) : std::prev(it));
        (*prevIt)->revealTableTo(table_name, node);
    } else {
        // 如过node本身没有份额，则各server将SharePair发送给node
         for (int i = 0; i < SHARE_PARTY_NUM; i++) {
            servers[i]->revealTableTo(table_name, node);
        }
    }
}

void Protocol::logicNOT(std::string x_name, std::string y_name) {
    // 自动解析y_name
    y_name = y_name == "" ? Node::notVarName(y_name) : y_name;

    doEachAsync([&](Server* server) {
        auto sp = std::any_cast<SharePair>(server->getVar(x_name));
        if (server->id == 0) {
            sp[0] = ~sp[0];
        } else if (server->id == 2) {
            sp[1] = ~sp[1];
        }
        server->setVar(y_name, sp);
    });
}

void Protocol::logicAND(std::string x_name, std::string y_name, std::string z_name) {
    doEachAsync([&](Server* server) {
        auto& x = std::any_cast<SharePair&>(server->getVar(x_name));
        auto& y = std::any_cast<SharePair&>(server->getVar(y_name));
        int zi = (x[0] & y[0]) ^ (x[0] & y[1]) ^ (x[1] & y[0]);
        server->setVar(z_name + "_i", zi);
        int to_id = server->id < 1 ? server->id + SHARE_PARTY_NUM - 1 : server->id - 1;
        VarMessage vmsg(server->id, (server->id - 1) % SHARE_PARTY_NUM, Command::RECEIVE_AND_SHARE, z_name, std::to_string(zi));
        server->sendMessageAndWait(to_id, vmsg);
    });
}

void Protocol::logicOR(std::string x_name, std::string y_name, std::string z_name) {
    std::string not_x_name = Node::notVarName(x_name);
    std::string not_y_name = Node::notVarName(y_name);
    std::string not_z_name = Node::notVarName(z_name);
    this->logicNOT(x_name, not_x_name);
    this->logicNOT(y_name, not_y_name);
    this->logicAND(not_x_name, not_y_name, not_z_name);
    this->logicNOT(not_z_name, z_name);

    // 删除中间量
    doEachAsync([&](Server* server) {
        server->deleteVar(not_x_name);
        server->deleteVar(not_y_name);
        if (not_z_name != not_x_name && not_z_name != not_y_name) {
            server->deleteVar(not_z_name);
        }
    });
}

void Protocol::subgraphMatchingOnlyMPC(const Table& query_graph, const std::string& result_name) {
    log("START PROTOCOL SUBGRAPH MATCHING ONLY WITH MPC");

    // 初始化N用于记录初始边表匹配结果的大小
    uint64_t N = 0;

    // 提取query的边表
    auto edges = query_graph.selectAsInt(query_graph.headers);
    // 计算query的对称节点
    auto symmetries = Utils::getSymmetries(edges);

    // 执行Pi+1 = Pi ⋈ pi+1, P0 = p0，pi的顺序与Q中边的顺序一致
    for (int i = 0; i < query_graph.data.size() - 1; i++) {
        std::string ss_Pi = "[P" + std::to_string(i) + "]";
        std::string ss_pip1 = "[p" + std::to_string(i + 1) + "]";
        std::string ss_Pip1;
        if (i == query_graph.data.size() - 2) { // 如果是最后一次，则将结果命名为result_name
            ss_Pip1 = result_name;
        } else {    // 否则为Pi+1
            ss_Pip1 = "[P" + std::to_string(i + 1) + "]";
        }

        // 解析pi的表头
        std::vector<std::string> current_edge_str;
        std::vector<std::any> current_edge = query_graph.data[i + 1];
        for (const auto &v : current_edge) {
            current_edge_str.push_back(std::to_string(std::any_cast<int>(v)));
        }

        doEachAsync([&](Server *server) {
            auto &G = std::any_cast<Table &>(server->getVar("[G]"));
            if (N == 0) {
                N = G.data.size();
            }

            // 第一次执行 P1 = P0(p0) ⋈ p1
            if (i == 0) {
                // 解析P0的表头
                std::vector<std::string> first_edge_str;
                std::vector<std::any> first_edge = query_graph.data[0];
                for (const auto &v : first_edge) {
                    first_edge_str.push_back(std::to_string(std::any_cast<int>(v)));
                }
                // 初始化待连接表
                Table P0(G);
                P0.setHeaders(first_edge_str);
                server->setVar("[P0]", P0);
                Table p1(G);
                p1.setHeaders(current_edge_str);
                server->setVar("[p1]", p1);
            } else {
                // 初始化pi和索引
                Table pip1(G);
                pip1.setHeaders(current_edge_str);
                server->setVar(ss_pip1, pip1);
            }
        });

        // 计算Pi+1的AGM上界，Pi+1实际上是query_graph的前i+1条边构成的子图，i从0开始
        // 初始化Pi+1的边表
        std::vector<std::vector<int>> Pip1_edges;
        // 从 query 中提取前i+1条边
        for (int j = 0; j <= i + 1; j++) {
            auto u = std::any_cast<int>(query_graph.data[j][0]);
            auto v = std::any_cast<int>(query_graph.data[j][1]);
            Pip1_edges.push_back({u, v});
        }

        // 记录测试前的时间
        start_ = std::chrono::steady_clock::now();
        // 使用第三方的安全连接功能，测试不同的连接方式和界限估计算法
        log("Start Join for " + ss_Pip1 + " = " + ss_Pi + " ⋈ " + ss_pip1);
        if (i == 0 || !use_third_party) {
            this->third_party->revealTable(ss_Pi);
        }
        this->third_party->revealTable(ss_pip1);
        this->third_party->join(ss_Pi, ss_pip1, ss_Pip1);
        // 记录测试后的时间
        end_ = std::chrono::steady_clock::now();
        log("Time cost: " + std::to_string(getDuration()) + " ms");

        log("Start Injectivity Check for match " + ss_Pip1);
        if (use_third_party) {
            // 使用第三方的检查匹配结果功能
            this->third_party->checkInjectivity(ss_Pip1);
        } else {
            // 使用匹配结果检查协议，需要先从ThirdParty拿到连接结果，然后自行处理
            // this->third_party->shareTable(Node::extractVarName(ss_Pip1), BINARY_SHARING, servers);
            // this->checkMatch(ss_Pip1, symmetries);
        }

        doEach([&](Server *server) {
            if (i == 0) {   // 第一轮删除初始化的P0，之后的Partial Result不会传回来
                server->deleteVar(ss_Pi);
            }
            server->deleteVar(ss_pip1);
        });

        if (i == query_graph.data.size() - 2) {     // 如果是最后一轮连接
            log("Start Symmetry Check for match " + ss_Pip1);
            if (use_third_party) {
                // 使用第三方的检查匹配结果功能
                this->third_party->checkSymmetry(ss_Pip1, symmetries);
                this->third_party->shareTable(Node::extractVarName(ss_Pip1), BINARY_SHARING, servers);
            } else {
                // 使用匹配结果检查协议，需要先从ThirdParty拿到连接结果，然后自行处理
                this->third_party->shareTable(Node::extractVarName(ss_Pip1), BINARY_SHARING, servers);
                this->checkMatch(ss_Pip1, symmetries);
            }
            this->third_party->deleteVar(Node::extractVarName(ss_Pip1));
        }
    }
    log("END PROTOCOL SUBGRAPH MATCHING");

}

void Protocol::subgraphMatchingNoJoinPlan(const Table &query_graph, const std::string& result_name) {
    log("START PROTOCOL SUBGRAPH MATCHING WITHOUT JOIN PLAN");
    // 初始化N用于记录初始边表匹配结果的大小
    uint64_t N = 0;
    // 初始化mf用于记录初始边表匹配结果的mf
    uint64_t mf = 0;

    // 提取query的边表
    auto edges = query_graph.selectAsInt(query_graph.headers);
    // 计算query的对称节点
    auto symmetries = Utils::getSymmetries(edges);

    // 执行Pi+1 = Pi ⋈ pi+1, P0 = p0，pi的顺序与Q中边的顺序一致
    for (int i = 0; i < query_graph.data.size() - 1; i++) {
        std::string ss_Pi = "[P" + std::to_string(i) + "]";
        std::string ss_pip1 = "[p" + std::to_string(i + 1) + "]";
        std::string ss_Pip1;
        if (i == query_graph.data.size() - 2) { // 如果是最后一次，则将结果命名为result_name
            ss_Pip1 = result_name;
        } else {    // 否则为Pi+1
            ss_Pip1 = "[P" + std::to_string(i + 1) + "]";
        }

        // 解析pi的表头
        std::vector<std::string> current_edge_str;
        std::vector<std::any> current_edge = query_graph.data[i + 1];
        for (const auto &v : current_edge) {
            current_edge_str.push_back(std::to_string(std::any_cast<int>(v)));
        }

        doEachAsync([&](Server *server) {
            auto &G = std::any_cast<Table &>(server->getVar("[G]"));
            if (N == 0) {
                N = G.data.size();
                mf = G.max_freqs[0];
            }

            auto index_arr = std::any_cast<std::vector<Index>>(server->getVar("G-idx"));

            // 第一次执行 P1 = P0(p0) ⋈ p1
            if (i == 0) {
                // 解析P0的表头
                std::vector<std::string> first_edge_str;
                std::vector<std::any> first_edge = query_graph.data[0];
                for (const auto &v : first_edge) {
                    first_edge_str.push_back(std::to_string(std::any_cast<int>(v)));
                }
                // 初始化待连接表
                Table P0(G);
                P0.setHeaders(first_edge_str);
                server->setVar("[P0]", P0);
                // 对属性a,b,ab都构建索引
                // index_arr[0].setAttrs(P0.headers);
                // index_arr[1].setAttrs({P0.headers[0]});
                // index_arr[2].setAttrs({P0.headers[1]});
                // 对属性a,b都构建索引
                index_arr[0].setAttrs({P0.headers[0]});
                index_arr[1].setAttrs({P0.headers[1]});
                server->setVar("P0-idx", index_arr);
                Table p1(G);
                p1.setHeaders(current_edge_str);
                server->setVar("[p1]", p1);
                // index_arr[0].setAttrs(p1.headers);
                // index_arr[1].setAttrs({p1.headers[0]});
                // index_arr[2].setAttrs({p1.headers[1]});
                // 对属性a,b都构建索引
                index_arr[0].setAttrs({p1.headers[0]});
                index_arr[1].setAttrs({p1.headers[1]});
                server->setVar("p1-idx", index_arr);
            } else {
                // 初始化pi和索引
                Table pip1(G);
                pip1.setHeaders(current_edge_str);
                server->setVar(ss_pip1, pip1);
                // index_arr[0].setAttrs(pip1.headers);
                // index_arr[1].setAttrs({pip1.headers[0]});
                // index_arr[2].setAttrs({pip1.headers[1]});
                // 对属性a,b都构建索引
                index_arr[0].setAttrs({pip1.headers[0]});
                index_arr[1].setAttrs({pip1.headers[1]});
                server->setVar(server->extractVarName(ss_pip1) + "-idx", index_arr);
            }
        });

        // 计算Pi+1的AGM上界，Pi+1实际上是query_graph的前i+1条边构成的子图，i从0开始
        // 初始化Pi+1的边表
        std::vector<std::vector<int>> Pip1_edges;
        // 从 query 中提取前i+1条边
        for (int j = 0; j <= i + 1; j++) {
            auto u = std::any_cast<int>(query_graph.data[j][0]);
            auto v = std::any_cast<int>(query_graph.data[j][1]);
            Pip1_edges.push_back({u, v});
        }
        // AGM_BOUND_ = computeAGMBound(Pip1_edges, N);
        // MIX_BOUND_ = Utils::mixedUpperBound(Pip1_edges, N, mf);

        // 记录测试前的时间
        start_ = std::chrono::steady_clock::now();
        // 使用第三方的安全连接功能，测试不同的连接方式和界限估计算法
        log("Start Join for " + ss_Pip1 + " = " + ss_Pi + " ⋈ " + ss_pip1);

        if (i == 0 || !use_third_party) {
            this->third_party->revealTable(ss_Pi);
        }

        if (i > 0) {    // 模拟MPC
            // 主动获取Pi
            this->third_party->revealTable(ss_Pi);
        }

        this->third_party->revealTable(ss_pip1);
        this->third_party->bucketJoin(ss_Pi, ss_pip1, ss_Pip1, MF_BOUND);
        // 记录测试后的时间
        end_ = std::chrono::steady_clock::now();
        log("Time cost: " + std::to_string(getDuration()) + " ms");

        // 同时各Server保存该表的索引
        auto indices = std::any_cast<std::vector<Index>>(this->third_party->getVar(Node::extractVarName(ss_Pip1) + "-idx"));
        doEachAsync([&](Server* server) {
            server->setVar(Node::extractVarName(ss_Pip1) + "-idx", indices);
        });

        log("Start Injectivity Check for match " + ss_Pip1);
        if (use_third_party) {
            // 使用第三方的检查匹配结果功能
            this->third_party->checkInjectivity(ss_Pip1);
        } else {
            // 使用匹配结果检查协议，需要先从ThirdParty拿到连接结果，然后自行处理
            // this->third_party->shareTable(Node::extractVarName(ss_Pip1), BINARY_SHARING, servers);
            // this->checkMatch(ss_Pip1, symmetries);
        }

        doEach([&](Server *server) {
            if (i == 0) {   // 第一轮删除初始化的P0，之后的Partial Result不会传回来
                server->deleteVar(ss_Pi);
            }
            server->deleteVar(ss_pip1);
        });

        if (i == query_graph.data.size() - 2) {     // 如果是最后一轮连接
            log("Start Symmetry Check for match " + ss_Pip1);
            if (use_third_party) {
                // 使用第三方的检查匹配结果功能
                this->third_party->checkSymmetry(ss_Pip1, symmetries);
                this->third_party->shareTable(Node::extractVarName(ss_Pip1), BINARY_SHARING, servers);
            } else {
                // 使用匹配结果检查协议，需要先从ThirdParty拿到连接结果，然后自行处理
                this->third_party->shareTable(Node::extractVarName(ss_Pip1), BINARY_SHARING, servers);
                this->checkMatch(ss_Pip1, symmetries);
            }
            this->third_party->deleteVar(Node::extractVarName(ss_Pip1));
        }

        else {      // 模拟MPC
            this->third_party->shareTable(Node::extractVarName(ss_Pip1), BINARY_SHARING, servers);
            this->third_party->deleteVar(Node::extractVarName(ss_Pip1));
        }
    }
    log("END PROTOCOL SUBGRAPH MATCHING");
}

// 正式版入口
void Protocol::subgraphMatching(const Table& query_graph, const std::string& result_name) {
    log("START PROTOCOL SUBGRAPH MATCHING");
    // 初始化N用于记录初始边表匹配结果的大小
    uint64_t N = 0;
    // 初始化mf用于记录初始边表匹配结果的mf
    uint64_t mf = 0;

    // 提取query的边表
    auto edges = query_graph.selectAsInt(query_graph.headers);

    // --------------------- PHASE 1: STAR DECOMPOSITION ---------------------
    log("PHASE 1: STAR DECOMPOSITION");

    std::vector<Utils::StarGraph> stars;
    // 将query分解为一系列星型子图
    std::ostringstream decomposition_oss;     // 用于接收分解结果打印
    stars = Utils::starDecomposition(edges, decomposition_oss);
    log(decomposition_oss.str());

    // 平衡处理
    std::ostringstream balance_oss;     // 用于接收平衡结果打印
    stars = Utils::balanceStars(stars, balance_oss);
    log(balance_oss.str());

    // 选择最大的星型子图
    const Utils::StarGraph& biggest = findbiggestStar(stars);
    const auto biggest_edges = biggest.toEdges();

    // --------------------- PHASE 2: STAR MATCHING ---------------------
    log("PHASE 2: STAR MATCHING");

    // 计算query的对称节点
    std::ostringstream symmetries_oss;     // 用于接收对称节点打印
    auto symmetries = Utils::getSymmetries(edges, symmetries_oss);
    log(symmetries_oss.str());

    // 使用BucketJoin构建最大的星型子图
    // 执行 stari+1 = stari ⋈ ei+1，i = {1, ..., biggest.size()}，star1 = e1，ei表示biggest中的第i条边，索引从1开始
    log("Start Multi-round BucktJoin");
    for (int i = 1; i < biggest.size(); i++) {
        // 解析每一轮构建星型子图的变量名
        std::string ss_stari = "[star" + std::to_string(i) + "]";
        std::string ss_eip1 = "[e" + std::to_string(i + 1) + "]";
        std::string ss_starip1 = "[star" + std::to_string(i + 1) + "]";

        log("Round " + std::to_string(i) + ": " + ss_starip1 + " = " + ss_stari + " ⋈ " + ss_eip1);

        // 各方初始化待连接的表
        if (i == 1) {
            log("Initialing " + ss_stari + " as [e1]");
        }
        doEachAsync([&](Server* server) {
            auto& G = std::any_cast<Table&>(server->getVar("[G]"));
            if (N == 0) {
                N = G.data.size();
                mf = G.max_freqs[0];
            }
            auto index_arr = std::any_cast<std::vector<Index>>(server->getVar("G-idx"));

            // 解析 ei+1 的表头 ei+1 = (root, neighbors[i])，注意e是从1开始计数，neighbors数组本身是从0开始计数
            std::vector<std::string> current_edge_str = {std::to_string(biggest.root), std::to_string(biggest.neighbors[i])};

            // 如果是第一轮连接，则需要额外解析star1，即e1的表头
            if (i == 1) {
                std::vector<std::string> first_edge_str = {std::to_string(biggest.root), std::to_string(biggest.neighbors[i - 1])};

                // 初始化待连接表star1
                Table S1(G);
                S1.setHeaders(first_edge_str);
                server->setVar(ss_stari, S1);
                // 构建索引，由于星型连接的连接键仅为root节点，故只对root构建索引
                auto root_index = index_arr[1];     // 0是(u,v)二维索引，1和2分别对应u和v的一维索引
                root_index.setAttrs({first_edge_str[0]});
                server->setVar("star1-idx", std::vector<Index>{root_index});
            }

            // 如果不是第一轮连接，则仅需要初始化待连接表ei+1即可
            Table eip1(G);
            eip1.setHeaders(current_edge_str);
            server->setVar(ss_eip1, eip1);
            // 构建索引
            auto root_index = index_arr[1];
            root_index.setAttrs({current_edge_str[0]});
            server->setVar(server->extractVarName(ss_eip1) + "-idx", std::vector<Index>{root_index});
        });

        // 记录测试前的时间
        start_ = std::chrono::steady_clock::now();
        // 使用第三方的安全BucketJoin连接功能
        if (i == 1 || !use_third_party) {   // 如果是第一轮连接，则需要传递，否则ThirdParty本地有保存
            this->third_party->revealTable(ss_stari);
        }
        this->third_party->revealTable(ss_eip1);
        this->third_party->bucketJoin(ss_stari, ss_eip1, ss_starip1, MIX_BOUND);
        // 记录测试后的时间
        end_ = std::chrono::steady_clock::now();
        log("Time cost: " + std::to_string(getDuration()) + " ms");
        
        log("Injectivity Check for match " + ss_starip1);
        if (use_third_party) {
            // 使用第三方的匹配结果检查功能，Servers保存Check后的中间星型结果
            this->third_party->checkInjectivity(ss_starip1); 
        } else {
            // 使用匹配结果检查协议，需要先从ThirdParty拿到连接结果，然后自行处理
            // this->third_party->shareTable(Node::extractVarName(ss_starip1), BINARY_SHARING, this->servers);
            // this->checkMatch(ss_starip1, symmetries);
        }

        // 拓扑复用
        // 检查stars中是否有当前size的star
        bool hasStar = false;
        for (auto star : stars) {
            if (star.size() == i + 1) {
                hasStar = true;
                break;
            }
        }
        if (hasStar) {  // 如果有，则拓扑复用
            log("Saving current star due to Topology Reuse");
            if (use_third_party) {
                this->third_party->shareTable(Node::extractVarName(ss_starip1), BINARY_SHARING, this->servers);
            }
        } else {    // 如果没有则填入空白
            if (use_third_party) {
                doEachAsync([&](Server *server) { 
                    server->setVar(ss_starip1, Table(Table::SHARE_PAIR_MODE));
                });
            }
        }
        // 同时各Server保存该表的索引
        auto indices = std::any_cast<std::vector<Index>>(this->third_party->getVar(Node::extractVarName(ss_starip1) + "-idx"));
        doEachAsync([&](Server* server) {
            server->setVar(Node::extractVarName(ss_starip1) + "-idx", indices);
        });

        // 各方处理中间变量
        log("Deleting intermediate vars");
        // Servers 删除中间变量[ei+1]
        doEachAsync([&](Server* server) {
            server->deleteVar(ss_eip1);
        });

        // ThirdParty 删除连接结果
        if (i == biggest.size() - 1) {
            this->third_party->deleteVar(Node::extractVarName(ss_starip1));
        }
    }

    // --------------------- PHASE 3: STAR ASSEMBLY ---------------------
    log("PHASE 3: STAR ASSEMBLY");
    // 处理完stars的匹配结果后，开始执行普通连接
    // 如果星型子图数量大于1，则计算 stars 之间的连接次序
    if (stars.size() > 1) {
        // 调用获得最优执行次序的方法获得置换数组
        auto perm = optimizeJoinOrder(stars, N);

        // 使用 perm 对 stars 进行排序
        auto stars_sorted = Utils::applyPermutation(perm, stars);

        std::ostringstream order_oss;
        order_oss << "Optimized star join order:";
        for (const auto& star : stars_sorted) {
            order_oss << "\n\t";
            star.print(order_oss);
        }
        log(order_oss.str());

        // 按照stars_sorted中次序依次连接星型子图
        log("Start Multi-round Join");
        for (int i = 0; i < stars_sorted.size() - 1; i++) {
            log("Round " + std::to_string(i + 1) + ": " + result_name + " = " + result_name + " ⋈ " + "[star" + std::to_string(stars_sorted[i + 1].size()) + "]");

            // 如果i = 0，额外初始化结果表为stars_sorted[0]
            if (i == 0) {
                log("Initialing " + result_name + " as [star" + std::to_string(stars_sorted[0].size()) + "]");
                doEachAsync([&](Server* server) {
                    auto& result = std::any_cast<Table&>(server->getVar("[star" + std::to_string(stars_sorted[0].size()) + "]"));
                    // result.setHeaders(stars_sorted[0].toStrings());
                    server->setVar(result_name, result);
                });
            }

            // 初始化stars_sorted[i + 1]
            doEachAsync([&](Server* server) {
                auto& star_match = std::any_cast<Table&>(server->getVar("[star" + std::to_string(stars_sorted[i + 1].size()) + "]"));
                star_match.setHeaders(stars_sorted[i + 1].toStrings());
            });

            // 计算当前连接结果的AGM上界
            std::vector<std::vector<int>> current_result_edges;
            for (int j = 0; j <= i + 1; j++) {
                auto star_edges = stars_sorted[j].toEdges();
                current_result_edges.insert(current_result_edges.end(), star_edges.begin(), star_edges.end());
            }
            // AGM_BOUND_ = computeAGMBound(current_result_edges, N);
            MIX_BOUND_ = Utils::mixedUpperBound(current_result_edges, N, mf, symmetries);
            // log("Mix bound is " + std::to_string(MIX_BOUND_));
            
            // 记录测试前的时间
            start_ = std::chrono::steady_clock::now();
            // 使用第三方的安全连接功能
            if (i == 0 || !use_third_party) {
                this->third_party->revealTable(result_name);
            } 
            this->third_party->revealTable("[star" + std::to_string(stars_sorted[i + 1].size()) + "]");
            // this->third_party->join(
            //     result_name,
            //     "[star" + std::to_string(stars_sorted[i + 1].size()) + "]",
            //     result_name,
            //     MIX_AGM_MF_BOUND
            // );
            this->third_party->join(
                result_name,
                "[star" + std::to_string(stars_sorted[i + 1].size()) + "]",
                result_name,
                MIX_BOUND
            );
            // 记录测试后的时间
            end_ = std::chrono::steady_clock::now();
            log("Time cost: " + std::to_string(getDuration()) + " ms");

            log("Injectivity Check for match " + result_name);
            if (use_third_party) {
                // 使用第三方的匹配结果检查功能
                this->third_party->checkInjectivity(result_name);
            } else {
                // 使用匹配结果检查协议，需要先从ThirdParty拿到连接结果，然后自行处理 
                // this->third_party->shareTable(Node::extractVarName(result_name), BINARY_SHARING, servers);
                // this->checkMatch(result_name, symmetries);
            }

            // // ThirdParty 共享并删除连接结果
            // if (i == stars_sorted.size() - 2) {
            //     this->third_party->shareTable(Node::extractVarName(result_name), BINARY_SHARING, servers);
            //     this->third_party->deleteVar(Node::extractVarName(result_name));
            // }
        }

    } 
    // 如果星型子图数量等于1，则biggest的结果即为最终结果
    else {    
        // 各方存储 biggest 作为最终结果
        doEachAsync([&](Server* server) {
            auto& result = std::any_cast<Table&>(server->getVar("[star" + std::to_string(biggest.size()) + "]"));
            server->setVar(result_name, result);
        });
        this->third_party->revealTable(result_name);
    }

    log("Symmetry Check for match " + result_name);
    if (use_third_party) {
        // 使用第三方的匹配结果检查功能
        this->third_party->checkSymmetry(result_name, symmetries);
        this->third_party->shareTable(Node::extractVarName(result_name), BINARY_SHARING, servers);
    } else {
        // 使用匹配结果检查协议，需要先从ThirdParty拿到连接结果，然后自行处理
        this->third_party->shareTable(Node::extractVarName(result_name), BINARY_SHARING, servers);
        this->checkMatch(result_name, symmetries);
    }
    this->third_party->deleteVar(Node::extractVarName(result_name));

    // 最后，删除所有中间star匹配结果
    log("Deleting intermediate vars");
    doEachAsync([&](Server *server) {
        for (int i = 1; i <= biggest.size(); i++) {
            server->deleteVar("[star" + std::to_string(i) + "]");
        }
    });

    log("END PROTOCOL SUBGRAPH MATCHING");
}

void Protocol::checkMatch(std::string table_name, std::vector<std::vector<int>> symmetries) {
    log("START PROTOCOL CONSTRAINT VERIFICATION");
    // 获取table的size
    int table_size = std::any_cast<Table>(servers[0]->getVar(table_name)).data.size();

    // 逐行进行匹配约束检查
    for (int row_idx = 0; row_idx < table_size; row_idx++) {
        // log("check " + std::to_string(row_idx) + " rows");
        // 1 重复约束检查
        
        // 使用Sort指令获得perm：发送table.data[i] = "[A]"，获得"[Pi]"
        doEachAsync([&](Server* server) {
            // 获取要检查的table
            auto& t = std::any_cast<Table&>(server->getVar(table_name));
            // 获取table的第i行数据
            std::vector<SharePair> A;
            for (auto& item : t.data[row_idx]) {
                A.push_back(std::any_cast<SharePair>(item));
            }
            server->setVar("[A]", A);
            // 发送 (SORT, [A]) 指令给ThirdParty
            VarMessage vmsg(server->id, third_party->id, Command::SORT_VECTOR, "[A]", SPs2String(A), "[Pi]");
            server->sendMessageAndWait(third_party->id, vmsg);
        });

        // 使用Perm指令对该行数据进行排序：发送"[Pi]", "[A]"，获得"[A']"
        doEachAsync([&](Server* server) {
            // 获取对应table的第i行数据的perm
            auto& Pi = std::any_cast<std::vector<SharePair>&>(server->getVar("[Pi]"));
            // 获取table的第i行数据
            auto& A = std::any_cast<std::vector<SharePair>&>(server->getVar("[A]"));
            // 发送 (PERM, [Pi], [A]) 指令给ThirdParty
            VarsMessage vsmsg(server->id, third_party->id, Command::PERM_VECTOR, {
                {"[Pi]", SPs2String(Pi)},
                {"[A]", SPs2String(A)}
            }, "[A']");
            server->sendMessageAndWait(third_party->id, vsmsg);
        });

        // 使用CompareNeighbor指令进行相邻对比：发送"[A']"，获得 "[D]"
        doEachAsync([&](Server* server) {
            // 获取排序后的[A']
            auto& A_prime = std::any_cast<std::vector<SharePair>&>(server->getVar("[A']"));
            // 发送 (CPN_EQU, [A]) 指令给ThirdParty
            VarMessage vmsg(server->id, third_party->id, Command::COMPARE_NEIGHBOR_EQ, "[A']", SPs2String(A_prime), "[D]");
            server->sendMessageAndWait(third_party->id, vmsg);
        });

        // 使用 OR_VEC 指令对向量求析取：发送 "[D]"，获得 "[d]"
        doEachAsync([&](Server* server) {
            // 获取标识符向量
            auto& D = std::any_cast<std::vector<SharePair>&>(server->getVar("[D]"));
            // 发送 (ORV, [D]) 指令给ThirdParty
            VarMessage vmsg(server->id, third_party->id, Command::OR_VECTOR, "[D]", SPs2String(D), "[d]");
            server->sendMessageAndWait(third_party->id, vmsg);
        });
        
        // 2 对称约束检查
        // 初始化symmetries中每一个对称组，并执行相邻大于比较
        std::vector<int> valid_symmetry_group_idx; // 记录有效对称组的索引
        // 如果存在对称节点的话才做
        if (symmetries.size() != 0) {
            for (int i = 0; i < symmetries.size(); i++) {
                // 依次处理每个对称组
                doEachAsync([&](Server *server) {
                    // 获取table，用于计算各点对应的列idx
                    auto &t = std::any_cast<Table &>(server->getVar(table_name));
                    // 获取table的第i行数据，即要处理的向量[A]
                    auto &A = std::any_cast<std::vector<SharePair> &>(server->getVar("[A]"));
                    // 提取改对称组
                    std::vector<SharePair> P_i;
                    for (const auto &node_id : symmetries[i]) { // symmetries中存的是节点的id，并不是列号
                        auto col_idx = t.getColumnIdx(std::to_string(node_id));
                        if (col_idx != -1) { // 提取对称组中存在于当前子匹配的节点映射
                            P_i.push_back(A[col_idx]);
                        }
                    }
                    // 使用CompareNeighbor_asc指令进行相邻大于对比：发送"[P_i]"，获得"[E_i]"，E_i[j] = 1 表示 A[j-1] >
                    // A[j]
                    if (P_i.size() > 1) {
                        VarMessage vmsg(server->id, third_party->id, Command::COMPARE_NEIGHBOR_GT,
                                        "[G_" + std::to_string(i) + "]", SPs2String(P_i),
                                        "[E_" + std::to_string(i) + "]");
                        server->sendMessageAndWait(third_party->id, vmsg);
                        // Server0辅助Protocol存储有效的对称组索引
                        if (server->id == 0) {
                            valid_symmetry_group_idx.push_back(i);
                        }
                    }
                });

                if (valid_symmetry_group_idx.back() == i) { // 如果当前对称组有效
                    // 将大于比较的结果求析取：发送 "[E_i]" 获得 "[e_i]"
                    doEachAsync([&](Server *server) {
                        // 获取相邻比较的输出[E_i]
                        auto &E_i =
                            std::any_cast<std::vector<SharePair> &>(server->getVar("[E_" + std::to_string(i) + "]"));
                        // 使用 OR_VEC 指令对向量求析取：发送 "[E_i]"，获得 "[e_i]"
                        VarMessage vmsg(server->id, third_party->id, Command::OR_VECTOR,
                                        "[E_" + std::to_string(i) + "]", SPs2String(E_i),
                                        "[e_" + std::to_string(i) + "]");
                        server->sendMessageAndWait(third_party->id, vmsg);
                    });
                }
            }

            // 使用 OR_VEC 指令对向量求析取：发送 "[E]"，获得 "[e]"，其中 [E] = {[e_1], [e_2], ...}
            doEachAsync([&](Server *server) {
                // 将 [e_i] 组合成数组 [E]
                std::vector<SharePair> E;
                // 对有效的对称组比较结果操作
                for (int i = 0; i < valid_symmetry_group_idx.size(); i++) {
                    auto &e_i = std::any_cast<SharePair &>(
                        server->getVar("[e_" + std::to_string(valid_symmetry_group_idx[i]) + "]"));
                    E.push_back(e_i);
                }
                VarMessage vmsg(server->id, third_party->id, Command::OR_VECTOR, "[E]", SPs2String(E), "[e]");
                server->sendMessageAndWait(third_party->id, vmsg);
            });
        }

        doEachAsync([&](Server* server) {
            auto& t = std::any_cast<Table&>(server->getVar(table_name));
            auto is_null = std::any_cast<SharePair&>(t.is_null[row_idx]);

            if (symmetries.size() != 0) {
                // 使用计算出的[d]和[e]以及原本的isNull，更新isNull[row_idx]
                auto &d = std::any_cast<SharePair &>(server->getVar("[d]"));
                auto &e = std::any_cast<SharePair &>(server->getVar("[e]"));

                VarMessage vmsg(server->id, third_party->id, Command::OR_VECTOR, "", SPs2String({d, e, is_null}),
                                "[isNull]");
                server->sendMessageAndWait(third_party->id, vmsg);
            } else {
                // 使用计算出的[d]以及原本的isNull，更新isNull[row_idx]
                auto &d = std::any_cast<SharePair &>(server->getVar("[d]"));

                VarMessage vmsg(server->id, third_party->id, Command::OR_VECTOR, "", SPs2String({d, is_null}),
                                "[isNull]");
                server->sendMessageAndWait(third_party->id, vmsg);
            }
            
            auto new_is_null = std::any_cast<SharePair>(server->getVar("[isNull]"));
            // 更新t.isNull[row_idx]的值
            t.is_null[row_idx] = new_is_null;
        });
        
        // 删除所有的中间变量
        doEachAsync([&](Server* server) {
            server->deleteVar("[A]");
            server->deleteVar("[Pi]");
            server->deleteVar("[A']");
            server->deleteVar("[D]");
            server->deleteVar("[d]");
            if (symmetries.size() != 0) {
                for (int i = 0; i < valid_symmetry_group_idx.size(); i++) {
                    server->deleteVar("[E_" + std::to_string(valid_symmetry_group_idx[i]) + "]");
                    server->deleteVar("[e_" + std::to_string(valid_symmetry_group_idx[i]) + "]");
                }
                server->deleteVar("[e]");
            }
            server->deleteVar("[isNull]");
        });
        log_process(row_idx + 1, table_size);
    }
}

void Protocol::checkMatch_new(std::string table_name, std::vector<std::vector<int>> symmetries) {
    log("START PROTOCOL CONSTRAINT VERIFICATION");

    // 获取table的size
    int table_size = std::any_cast<Table>(servers[0]->getVar(table_name)).data.size();

    // 逐行进行匹配约束检查
    for (int row_idx = 0; row_idx < table_size; row_idx++) {
        // Injectivity Check
        // 各方准备表T的第i行数据t
        doEachAsync([&](Server* server) {
            auto& T = std::any_cast<Table&>(server->getVar(table_name));
            std::vector<SharePair> t;
            for (auto& item : T.data[row_idx]) {
                t.push_back(std::any_cast<SharePair>(item));
            }
            server->setVar("[t]", t);
        });
        
        this->third_party->revealVec("[t]");
        this->third_party->sort("[t]", "[t']");
        this->third_party->hasNeighborEqual("[t']", "[d]");
        this->third_party->shareInt("d", BINARY_SHARING, servers);

        // Symmetry Check
        // 初始化标志位e为0
        doEachAsync([&](Server* server) {
            server->setVar("[e]", SharePair(std::array<int, 2>({0, 0}), BINARY_SHARING));
        });

        // 对每一个对称组验证
        for (int i = 0; i < symmetries.size(); i++) {
            doEachAsync([&](Server* server) {
                // 各方准备表T的第i行数据t
                auto& T = std::any_cast<Table&>(server->getVar(table_name));
                auto& t = std::any_cast<std::vector<SharePair>&>(server->getVar("[t]"));
                // 提取对称组Si
                std::vector<SharePair> Si;
                for (const auto& id : symmetries[i]) {
                    auto col_idx = T.getColumnIdx(std::to_string(id));
                    if (col_idx != -1) {
                        Si.push_back(t[col_idx]);
                    }
                }
                server->setVar("[Si]", Si);
            });

            this->third_party->revealVec("[Si]");
            this->third_party->isOrdered("[Si]", "[ei]");
            this->third_party->shareInt("ei", BINARY_SHARING, servers);
            this->logicOR("[e]", "[ei]", "[e]");
        }

        // Update isNull
        doEachAsync([&](Server* server) {
            auto& T = std::any_cast<Table&>(server->getVar(table_name));
            auto& isNull = std::any_cast<SharePair&>(T.is_null[row_idx]);
            server->setVar("[isNull]", isNull);
        });

        this->logicOR("[isNull]", "[d]", "[isNull]");
        this->logicOR("[isNull]", "[e]", "[isNull]");

        doEachAsync([&](Server* server) {
            auto& T = std::any_cast<Table&>(server->getVar(table_name));
            auto isNull_new = std::any_cast<SharePair>(server->getVar("[isNull]"));
            T.is_null[row_idx] = isNull_new;
        });

        // 删除中间变量
        doEachAsync([&](Server* server) {
            server->deleteVar("[t]");
            server->deleteVar("[d]");
            server->deleteVar("[e]");
            server->deleteVar("[Si]");
            server->deleteVar("[ei]");
            server->deleteVar("[isNull]");
        });

        this->third_party->deleteVar("t");
        this->third_party->deleteVar("t'");
        this->third_party->deleteVar("d");
        this->third_party->deleteVar("Si");
        this->third_party->deleteVar("ei");

        log_process(row_idx + 1, table_size);
    }
    log("END PROTOCOL CONSTRAINT VERIFICATION");
}

void Protocol::log(const std::string& log) const {
    if (!isLog) { return; }
    std::cout << "\033[1m" << "<LOG>[PROTOCOL]: " << log << "\033[0m" << std::endl;
}

void Protocol::log_process(size_t current, size_t total) const {
    if (!isLog) { return; }
    
    if (total == 0) return;

    // 计算当前百分比（取整）
    int currentPrecentage = static_cast<int>((current * 100.0) / total);

    // 静态变量用于记录上一次输出的百分比，保证在同一个循环中同一百分比只打印一次
    static int lastPrintedPercentage = -1;

    if (currentPrecentage != lastPrintedPercentage) {
        std::lock_guard<std::mutex> guard(ptl_print_mutex);
        lastPrintedPercentage = currentPrecentage;
        std::cout << "\r" << "\033[1m" << "<LOG>[PROTOCOL]: " << currentPrecentage << "% complete (total: " << total << ")" << "\033[0m" << std::flush;   
    }

    if (current == total) {
        std::cout << std::endl;
    }
}

uint64_t Protocol::computeAGMBound(const std::vector<std::vector<int>>& edges, uint64_t size) {
    std::set<std::string> V;
    std::vector<std::set<std::string>> E;
    std::vector<uint64_t> N;

    for (const auto& edge : edges) {
        auto u = std::to_string(edge[0]);
        auto v = std::to_string(edge[1]);
        V.insert(u);
        V.insert(v);
        E.push_back(std::set<std::string>({u, v}));
        N.push_back(size);
    }

    return Utils::computeAGMBound(V, E, N);
    // std::cout << "Finish compute AGM Bound: " << AGM_BOUND_ << std::endl;
}

std::vector<int> Protocol::optimizeJoinOrder(const std::vector<Utils::StarGraph>& stars, uint64_t size) {
    int n = stars.size();
    if (n <= 2) {
        // 如果只有0或1个星型子图，直接返回顺序索引
        std::vector<int> perm;
        for (int i = 0; i < n; ++i) {
            perm.push_back(i);
        }
        return perm;
    }

    // 初始化排列数组
    std::vector<int> perm;
    // 初始化已经被选择的星型子图
    std::vector<bool> selected(n, false);
    
    // 修改：找到最佳的前两个星型子图
    int first_star = -1, second_star = -1;
    uint64_t min_initial_size = UINT64_MAX;

    for (int i = 0; i < n; i++) {
        for (int j = i + 1; j < n; j++) {
            // 构建两个星型子图连接后的边集合
            std::vector<std::vector<int>> temp_edges = stars[i].toEdges();
            std::vector<std::vector<int>> next_star_edges = stars[j].toEdges();
            
            // 合并边集合
            for (const auto& edge : next_star_edges) {
                temp_edges.push_back(edge);
            }

            // 估计连接后的结果大小
            uint64_t temp_size = computeAGMBound(temp_edges, size);

            if (temp_size < min_initial_size) {
                min_initial_size = temp_size;
                first_star = i;
                second_star = j;
            }
        }
    }

    // 将最优前两个星型子图添加到结果中
    perm.push_back(first_star);
    perm.push_back(second_star);
    selected[first_star] = true;
    selected[second_star] = true;

    // 当前已经连接的星型子图的边集合
    std::vector<std::vector<int>> current_edges;
    for (const auto& edge : stars[first_star].toEdges()) {
        current_edges.push_back(edge);
    }
    for (const auto& edge : stars[second_star].toEdges()) {
        current_edges.push_back(edge);
    }

    // 贪心选择剩下的星型子图
    for (int i = 2; i < n; i++) {
        int best_next_star = -1;
        uint64_t min_size = UINT64_MAX;

        // 尝试每个未选择的星型子图作为下一个连接对象
        for (int j = 0; j < n; j++) {
            if (selected[j]) continue;

            // 尝试构建连接后的边集合
            auto temp_edges = current_edges;
            auto next_star_edges = stars[j].toEdges();
            
            // 合并边集合
            for (const auto& edge : next_star_edges) {
                temp_edges.push_back(edge);
            }

            // 估计连接后的结果大小
            uint64_t temp_size = computeAGMBound(temp_edges, size);

            // 如果找到更优的则更新选择
            if (temp_size < min_size) {
                min_size = temp_size;
                best_next_star = j;
            }
        }

        // 将最佳选择添加到结果中
        perm.push_back(best_next_star);
        selected[best_next_star] = true;

        // 更新当前边集合
        auto next_star_edges = stars[best_next_star].toEdges();
        for (const auto& edge : next_star_edges) {
            current_edges.push_back(edge);
        }
    }

    return perm;
}