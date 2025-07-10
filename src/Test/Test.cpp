#include "Test.h"

#include <thread>
#include <unistd.h>
#include <limits.h>    // PATH_MAX

#include "../Node/Server.h"
#include "../Node/ThirdParty.h"
#include "../Protocol/Protocol.h"
#include "../Statistic/Statistic.h"

static const std::string PROJECT_PATH = PROJECT_SOURCE_DIR;

// uint64_t AGM_BOUND_ = CARTESIAN_BOUND;

void basic_test() {
    // 创建节点对象
    Server s0(0, "SERVER 0");
    Server s1(1, "SERVER 1");
    Server s2(2, "SERVER 2");
    Node o(3, "OWNER");
    ThirdParty t(4, "THIRDPARTY", {&s0, &s1, &s2});
    
    //为节点对象创建线程，开始监听
    std::thread t0(&Node::startlistening, &s0);
    std::thread t1(&Node::startlistening, &s1);
    std::thread t2(&Node::startlistening, &s2);
    std::thread to(&Node::startlistening, &o);
    std::thread tt(&Node::startlistening, &t);
    std::cout << "all servers are listening" << std::endl;
    sleep(1);

    // 连接各节点
    Node::connectToPeers({&s0, &s1, &s2, &o, &t});
    std::cout << "all servers have connected to peers" << std::endl;

    // 开始交互
    int a = 7;
    o.setVar("a", a);
    o.shareInt("a", BINARY_SHARING, {&s0, &s1, &s2});
    o.shareInt("b", 11, BINARY_SHARING, {&s0, &s1, &s2});
    // t.add("[a]", "[b]", "[c]", BINARY_SHARING);

    s0.printVars();
    s1.printVars();
    s2.printVars();
    o.printVars();
    t.printVars();

    // 主动关闭所有节点，先关闭依赖其他节点的对象
    o.stop();
    t.stop();
    s0.stop();
    s1.stop();
    s2.stop();

    // Clean up threads
    t0.join();
    t1.join();
    t2.join();
    to.join();
    tt.join();
}

void table_test() {
    // 创建节点对象
    Server s0(0, "SERVER 0");
    Server s1(1, "SERVER 1");
    Server s2(2, "SERVER 2");
    Node o(3, "OWNER");
    ThirdParty t(4, "THIRDPARTY", {&s0, &s1, &s2});
    
    //为节点对象创建线程，开始监听
    std::thread t0(&Node::startlistening, &s0);
    std::thread t1(&Node::startlistening, &s1);
    std::thread t2(&Node::startlistening, &s2);
    std::thread to(&Node::startlistening, &o);
    std::thread tt(&Node::startlistening, &t);
    std::cout << "all servers are listening" << std::endl;
    sleep(1);

    // 连接各节点
    Node::connectToPeers({&s0, &s1, &s2, &o, &t});
    std::cout << "all servers have connected to peers" << std::endl;

    Table table1(Table::INT_MODE);
    Table table2(Table::INT_MODE);
    try {
        table1.readFromFile("/home/tsy529/WorkSpace/cpp/ppsm/data/table1_data.csv");
        table2.readFromFile("/home/tsy529/WorkSpace/cpp/ppsm/data/table2_data.csv");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    o.setVar("T1", table1);
    o.shareTable("T1", {ARITHMETIC_SHARING, BINARY_SHARING, BINARY_SHARING}, {&s0, &s1, &s2});

    o.setVar("T2", table2);
    o.shareTable("T2", {ARITHMETIC_SHARING, BINARY_SHARING}, {&s0, &s1, &s2});

    t.join("[T1]", "[T2]", "[T3]");
    t.revealTable("[T3]");

    s0.printVars();
    s1.printVars();
    s2.printVars();
    o.printVars();
    t.printVars();

    // Clean up threads
    t0.join();
    t1.join();
    t2.join();
    to.join();
    tt.join();
}

void graph_submatch_test() {
    // 创建节点对象
    Server s0(0, "SERVER 0");
    Server s1(1, "SERVER 1");
    Server s2(2, "SERVER 2");
    Node o(3, "OWNER");
    ThirdParty t(4, "THIRDPARTY", {&s0, &s1, &s2});
    
    //为节点对象创建线程，开始监听
    std::thread t0(&Node::startlistening, &s0);
    std::thread t1(&Node::startlistening, &s1);
    std::thread t2(&Node::startlistening, &s2);
    std::thread to(&Node::startlistening, &o);
    std::thread tt(&Node::startlistening, &t);
    std::cout << "all servers are listening" << std::endl;
    sleep(1);

    // 连接各节点
    Node::connectToPeers({&s0, &s1, &s2, &o, &t});
    std::cout << "all servers have connected to peers" << std::endl;

    Protocol protocol({&s0, &s1, &s2}, &t);

    // 读取表
    Table data_graph(Table::INT_MODE);
    Table query_graph(Table::INT_MODE);
    try {
        data_graph.readFromFile(PROJECT_PATH + "/data/data_graph.csv");
        query_graph.readFromFile(PROJECT_PATH + "/data/query_graph_1.csv");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
    std::cout << "Data Graph:\n" << data_graph.toString() << std::endl;
    std::cout << "Query Graph:\n" << query_graph.toString() << std::endl;

    // 生成表的附带信息
    auto edges = query_graph.selectAsInt(query_graph.headers);

    // 定义查询图中对称节点组
    std::vector<std::vector<int>> symmetries = Utils::getSymmetries(edges);

    // query 星型分解
    std::vector<Utils::StarGraph> decomposition = Utils::starDecomposition(edges);

    // 构造data_graph的DP索引
    std::vector<Range> ranges(data_graph.headers.size(), std::make_pair(1, 6));
    std::vector<int> widths(data_graph.headers.size(), 2);
    auto records = data_graph.selectAsInt(data_graph.headers);
    Histogram hst_uv(data_graph.headers, records, ranges);
    Histogram hst_u = hst_uv.reduction({hst_uv.getAttrs()[0]});
    Histogram hst_v = hst_uv.reduction({hst_uv.getAttrs()[1]});
    Index idx_uv(hst_uv, widths, false);
    Index idx_u(hst_u, {widths[0]}, false);
    Index idx_v(hst_v, {widths[1]}, false);

    // Owner把数据图共享给Servers
    o.setVar("G", data_graph);
    o.shareTable("G", {BINARY_SHARING, BINARY_SHARING}, {&s0, &s1, &s2});

    // 给Servers设置data_graph的DP-Index
    s0.setTableIndex("[G]", idx_uv);
    s0.setTableIndex("[G]", idx_u);
    s0.setTableIndex("[G]", idx_v);
    s1.setTableIndex("[G]", idx_uv);
    s1.setTableIndex("[G]", idx_u);
    s1.setTableIndex("[G]", idx_v);
    s2.setTableIndex("[G]", idx_uv);
    s2.setTableIndex("[G]", idx_u);
    s2.setTableIndex("[G]", idx_v);

    // Servers开始执行子图匹配协议，结果表名为[PR]
    protocol.subgraphMatching(query_graph, "[PR]");

    // 由第三方代为获取连接结果
    t.revealTable("[PR]");
    // t.printVars();

    // s0.printVarsInFile();
    // s1.printVarsInFile();
    // s2.printVarsInFile();
    // o.printVarsInFile();
    t.printVarsInFile();

    // 主动关闭所有节点，先关闭依赖其他节点的对象
    o.stop();
    t.stop();
    s0.stop();
    s1.stop();
    s2.stop();

    // Clean up threads
    t0.join();
    t1.join();
    t2.join();
    to.join();
    tt.join();
}

void join_plan_test() {
    // 创建节点对象
    Server s0(0, "SERVER 0");
    Server s1(1, "SERVER 1");
    Server s2(2, "SERVER 2");
    Node o(3, "OWNER");
    ThirdParty t(4, "THIRDPARTY", {&s0, &s1, &s2});
    
    //为节点对象创建线程，开始监听
    std::thread t0(&Node::startlistening, &s0);
    std::thread t1(&Node::startlistening, &s1);
    std::thread t2(&Node::startlistening, &s2);
    std::thread to(&Node::startlistening, &o);
    std::thread tt(&Node::startlistening, &t);
    std::cout << "all servers are listening" << std::endl;
    sleep(1);

    // 连接各节点
    Node::connectToPeers({&s0, &s1, &s2, &o, &t});
    std::cout << "all servers have connected to peers" << std::endl;

    Protocol protocol({&s0, &s1, &s2}, &t);

    // 读取表
    Table data_graph(Table::INT_MODE);
    Table query_graph(Table::INT_MODE);
    try {
        data_graph.readFromFile(PROJECT_PATH + "/data/data_graph2.csv");
        query_graph.readFromFile(PROJECT_PATH + "/data/query_graph2.csv");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
    std::cout << "Data Graph:\n" << data_graph.toString() << std::endl;
    std::cout << "Query Graph:\n" << query_graph.toString() << std::endl;

    // 生成表的附带信息
    auto edges = query_graph.selectAsInt(query_graph.headers);

    // 构造data_graph的DP索引（为了直观的测试Join，此处的索引是不加噪的）
    std::vector<Range> ranges = data_graph.getAllColumnValueRange();
    std::vector<int> widths(data_graph.headers.size(), (ranges[0].second - ranges[0].first + 1) / 3);
    auto records = data_graph.selectAsInt(data_graph.headers);
    Histogram hst_uv(data_graph.headers, records, ranges);
    Histogram hst_u = hst_uv.reduction({hst_uv.getAttrs()[0]});
    Histogram hst_v = hst_uv.reduction({hst_uv.getAttrs()[1]});
    Index idx_uv(hst_uv, widths, false);
    Index idx_u(hst_u, {widths[0]}, false);
    Index idx_v(hst_v, {widths[1]}, false);

    // 计算query的AGM上界
    // 初始化超图点集、边集
    std::set<std::string> V;
    std::vector<std::set<std::string>> E;
    std::vector<uint64_t> N;
    for (const auto& row : query_graph.data) {
        auto u = std::to_string(std::any_cast<int>(row[0]));
        auto v = std::to_string(std::any_cast<int>(row[1]));
        V.insert(u);
        V.insert(v);
        E.push_back(std::set<std::string>({u, v}));
        N.push_back(data_graph.data.size());
    }
    AGM_BOUND_ = Utils::computeAGMBound(V, E, N);
    std::cout << "Finish compute AGM Bound: " << AGM_BOUND_ << std::endl;

    // Owner把数据图共享给Servers
    o.setVar("G", data_graph);
    o.shareTable("G", {BINARY_SHARING, BINARY_SHARING}, {&s0, &s1, &s2});

    // 给Servers设置data_graph的DP-Index
    s0.setTableIndex("[G]", idx_uv);
    s0.setTableIndex("[G]", idx_u);
    s0.setTableIndex("[G]", idx_v);
    s1.setTableIndex("[G]", idx_uv);
    s1.setTableIndex("[G]", idx_u);
    s1.setTableIndex("[G]", idx_v);
    s2.setTableIndex("[G]", idx_uv);
    s2.setTableIndex("[G]", idx_u);
    s2.setTableIndex("[G]", idx_v);

    // Servers开始执行子图匹配协议，结果表名为[PR]
    protocol.use_third_party = true;
    protocol.subgraphMatching(query_graph, "[PR]");

    t.revealTable("[PR]");
    auto& result = std::any_cast<Table&>(t.getVar("PR"));
    result.normalize();
    t.printVarsInFile();

    // 主动关闭所有节点，先关闭依赖其他节点的对象
    o.stop();
    t.stop();
    s0.stop();
    s1.stop();
    s2.stop();

    // Clean up threads
    t0.join();
    t1.join();
    t2.join();
    to.join();
    tt.join();
}

void table_index_test() {
    Table table(Table::INT_MODE);
    try {
        table.readFromFile(PROJECT_PATH + "/data/table2_data.csv");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    std::vector<std::string> attrs = {"ID", "Class"};
    std::vector<Range> ranges = {{1, 50}, {0, 7}};

    auto rawRecords = table.select(attrs);
    std::vector<std::vector<int>> records;
    for (auto& item : rawRecords) {
        std::vector<int> record;
        record.push_back(std::any_cast<int>(item[0]));
        record.push_back(std::any_cast<int>(item[1]));
        records.push_back(record);
    }

    Histogram h(attrs, records, ranges);
    std::cout << h.toString() << std::endl;

    Histogram ph = h.reduction({attrs[1]});
    std::cout << ph.toString() << std::endl;

    HierarchicalHistogram hh(ph.getCounts(), ph.getRanges()[0]);
    std::cout << "finish hh" << std::endl;
    std::cout << hh.toStringByTree() << std::endl;

    HierarchicalHistogram hh_noise = hh.addDPNoise(1, 1);
    std::cout << "finish hh_noise" << std::endl;
    std::cout << hh_noise.toStringByTree() << std::endl;
    // std::cout << hh.toStringByLevel() << std::endl;

    Index idx(ph, {3}, true);
    std::cout << idx.toString() << std::endl;

    // idx.addNoise();
    // std::cout << idx.toString() << std::endl;

    // Index reductionIdx = idx.reduction(attrs[1]);
    // std::cout << reductionIdx.toString() << std::endl;

}

void table_bucketjoin_test() {
    // 创建节点对象
    Server s0(0, "SERVER 0");
    Server s1(1, "SERVER 1");
    Server s2(2, "SERVER 2");
    Node o(3, "OWNER");
    ThirdParty t(4, "THIRDPARTY", {&s0, &s1, &s2});

    //为节点对象创建线程，开始监听
    std::thread t0(&Node::startlistening, &s0);
    std::thread t1(&Node::startlistening, &s1);
    std::thread t2(&Node::startlistening, &s2);
    std::thread to(&Node::startlistening, &o);
    std::thread tt(&Node::startlistening, &t);
    std::cout << "all servers are listening" << std::endl;
    sleep(1);

    // 连接各节点
    Node::connectToPeers({&s0, &s1, &s2, &o, &t});
    std::cout << "all servers have connected to peers" << std::endl;

    // 读取表
    Table table1(Table::INT_MODE);
    Table table2(Table::INT_MODE);
    try {
        table1.readFromFile(PROJECT_PATH + "/data/table1_data.csv");
        table2.readFromFile(PROJECT_PATH + "/data/table2_data.csv");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    // 创建索引
    std::vector<std::string> attrs1 = {"ID"};
    std::vector<Range> ranges1 = {{1, 10}};
    std::vector<int> widths1 = {2};
    auto records1 = table1.selectAsInt(attrs1);
    Histogram hst1(attrs1, records1, ranges1);
    Index index1(hst1, widths1, false);
    std::cout << index1.toString() << std::endl;

    std::vector<std::string> attrs2 = {"ID"};
    std::vector<Range> ranges2 = {{1, 10}};
    std::vector<int> widths2 = {2};
    auto records2 = table2.selectAsInt(attrs2);
    Histogram hst2(attrs2, records2, ranges2);
    Index index2(hst2, widths2, false);
    std::cout << index2.toString() << std::endl;

    // Owner共享数据表
    o.setVar("T1", table1);
    o.shareTable("T1", {ARITHMETIC_SHARING, BINARY_SHARING, BINARY_SHARING}, {&s0, &s1, &s2});
    o.setVar("T2", table2);
    o.shareTable("T2", {ARITHMETIC_SHARING, BINARY_SHARING}, {&s0, &s1, &s2});

    // Servers存储索引
    o.setTableIndex("T1", index1);
    o.setTableIndex("T2", index1);
    s0.setTableIndex("T1", index1);
    s0.setTableIndex("T2", index2);
    s1.setTableIndex("T1", index1);
    s1.setTableIndex("T2", index2);
    s2.setTableIndex("T1", index1);
    s2.setTableIndex("T2", index2);

    t.bucketJoin("[T1]", "[T2]", "[T3]");
    t.revealTable("[T3]");

    s0.printVars();
    // s1.printVars();
    // s2.printVars();
    o.printVars();
    t.printVars();

    // Clean up threads
    t0.join();
    t1.join();
    t2.join();
    to.join();
    tt.join();
}

void get_symmetries_test() {
    std::vector<std::vector<int>> edges1 = {{1,2}, {1,4}, {2,3}, {2,4}, {3,4}};
    std::cout << "测试用例1结果：" << std::endl;
    std::vector<std::vector<int>> result1 = Utils::getSymmetries(edges1);
    

    std::vector<std::vector<int>> edges2 = {{1,2}, {1,3}, {1,4}, {1,5}};
    std::cout << "测试用例2结果：" << std::endl;
    std::vector<std::vector<int>> result2 = Utils::getSymmetries(edges2);

    std::vector<std::vector<int>> edges3 = {{1,2}, {2,3}, {3,4}, {4,5}};
    std::cout << "测试用例3结果：" << std::endl;
    std::vector<std::vector<int>> result3 = Utils::getSymmetries(edges3);

    std::vector<std::vector<int>> edges4 = {{1,2}, {2,3}, {3,4}, {4,5}, {5,6}};
    std::cout << "测试用例4结果：" << std::endl;
    std::vector<std::vector<int>> result4 = Utils::getSymmetries(edges4);
}

void compute_AGMBound_test() {
    /*
    // 样例1：三角形查询
    std::set<std::string> V1 = {"A", "B", "C"};
    std::vector<std::set<std::string>> E1 = {{"A", "B"}, {"B", "C"}, {"A", "C"}};
    std::vector<uint64_t> N1 = {1000, 1000, 1000};
    std::vector<uint64_t> MF1 = {10, 20, 5};

    uint64_t agm_uni1 = Utils::computeAGMBound(V1, E1, N1);
    uint64_t agm_step1 = Utils::stepwiseAGMBound(V1, E1, N1);
    uint64_t mf_uni1 = Utils::computeMFBound(MF1, N1);
    uint64_t mf_step1 = Utils::stepwiseMFBound(MF1, N1);
    std::cout << "[Triangle Query]\n"
              << "  Unified AGM Bound:  " << agm_uni1 << "\n"
              << "  Stepwise AGM Bound: " << agm_step1 << "\n"
              << "  Unified MF Bound:  " << mf_uni1 << "\n"
              << "  Stepwise MF Bound:  " << mf_step1 << "\n"
              << "\n\n";

    // 样例2：链式查询 R(A,B)⨝S(B,C)⨝T(C,D)
    std::set<std::string> V2 = {"A", "B", "C", "D"};
    std::vector<std::set<std::string>> E2 = {{"A", "B"}, {"B", "C"}, {"C", "D"}};
    std::vector<uint64_t> N2 = {1000, 1000, 1000};
    std::vector<uint64_t> MF2 = {10, 20, 5};

    uint64_t agm_uni2 = Utils::computeAGMBound(V2, E2, N2);
    uint64_t agm_step2 = Utils::stepwiseAGMBound(V2, E2, N2);
    uint64_t mf_uni2 = Utils::computeMFBound(MF2, N2);
    uint64_t mf_step2 = Utils::stepwiseMFBound(MF2, N2);
    std::cout << "[Chain Query]\n"
              << "  Unified AGM Bound:  " << agm_uni2 << "\n"
              << "  Stepwise AGM Bound: " << agm_step2 << "\n"
              << "  Unified MF Bound:  " << mf_uni2 << "\n"
              << "  Stepwise MF Bound:  " << mf_step2 << "\n"
              << "\n\n";

    // 样例3：自定义查询 R(1,2)⨝R(1,4)⨝R(2,3)⨝R(2,4)⨝R(3,4)
    std::set<std::string> V3 = {"1", "2", "3", "4"};
    std::vector<std::set<std::string>> E3 = {{"1", "2"}, {"1", "4"}, {"2", "3"}, {"2", "4"}, {"3", "4"}};
    std::vector<uint64_t> N3 = {18, 18, 18, 18, 18};
    std::vector<uint64_t> MF3 = {4, 4, 4, 4, 4};

    uint64_t agm_uni3 = Utils::computeAGMBound(V3, E3, N3);
    uint64_t agm_step3 = Utils::stepwiseAGMBound(V3, E3, N3);
    uint64_t mf_uni3 = Utils::computeMFBound(MF3, N3);
    uint64_t mf_step3 = Utils::stepwiseMFBound(MF3, N3);
    std::cout << "[DIY Query]\n"
              << "  Unified AGM Bound:  " << agm_uni3 << "\n"
              << "  Stepwise AGM Bound: " << agm_step3 << "\n"
              << "  Unified MF Bound:  " << mf_uni3 << "\n"
              << "  Stepwise MF Bound:  " << mf_step3 << "\n"
              << "\n\n";
    */

    // 样例4：自定义查询 R(a,b) ⋈ R(a,c) ⋈ R(c,d) ⋈ R(b,d) ⋈ R(b,e) ⋈ R(b,f) ⋈ R(e,f) ⋈ R(f,d)
    std::set<std::string> V4 = {"a", "b", "c", "d", "e", "f"};
    std::vector<std::set<std::string>> E4 = {{"a", "b"}, {"a", "c"}, {"b", "d"}, {"b", "e"}, {"b", "f"}, {"c", "d"}, {"d", "f"}, {"e", "f"}};
    std::vector<uint64_t> N4 = {18, 18, 18, 18, 18, 18, 18, 18};

    uint64_t agm_uni4 = Utils::computeAGMBound(V4, E4, N4);
    std::cout << "[DIY Query]\n"
              << "  Unified AGM Bound:  " << agm_uni4 << "\n"
              << "\n\n";


    // 样例5：自定义查询 R(a,b) ⋈ R(a,c) ⋈ R(c,d) ⋈ R(b,d) ⋈ R(b,e) ⋈ R(b,f)
    std::set<std::string> V5 = {"a", "b", "c", "d", "e", "f"};
    std::vector<std::set<std::string>> E5 = {{"a", "b"}, {"a", "c"}, {"b", "d"}, {"b", "e"}, {"b", "f"}, {"c", "d"}};
    std::vector<uint64_t> N5 = {18, 18, 18, 18, 18, 18};

    uint64_t agm_uni5 = Utils::computeAGMBound(V5, E5, N5);
    std::cout << "[DIY Query]\n"
              << "  Unified AGM Bound:  " << agm_uni5 << "\n"
              << "\n\n";

    // 样例6：自定义查询 R(c,d) ⋈ R(b,d) ⋈ R(b,e) ⋈ R(b,f) ⋈ R(e,f) ⋈ R(f,d)
    std::set<std::string> V6 = {"a", "b", "c", "d", "e", "f"};
    std::vector<std::set<std::string>> E6 = {{"b", "d"}, {"b", "e"}, {"b", "f"}, {"c", "d"}, {"d", "f"}, {"e", "f"}};
    std::vector<uint64_t> N6 = {18, 18, 18, 18, 18, 18};

    uint64_t agm_uni6 = Utils::computeAGMBound(V6, E6, N6);
    std::cout << "[DIY Query]\n"
              << "  Unified AGM Bound:  " << agm_uni6 << "\n"
              << "\n\n";

}

void checkMatch_protocol_test() {
    // 创建节点对象
    Server s0(0, "SERVER 0");
    Server s1(1, "SERVER 1");
    Server s2(2, "SERVER 2");
    Node o(3, "OWNER");
    ThirdParty t(4, "THIRDPARTY", {&s0, &s1, &s2});
    
    //为节点对象创建线程，开始监听
    std::thread t0(&Node::startlistening, &s0);
    std::thread t1(&Node::startlistening, &s1);
    std::thread t2(&Node::startlistening, &s2);
    std::thread to(&Node::startlistening, &o);
    std::thread tt(&Node::startlistening, &t);
    std::cout << "all servers are listening" << std::endl;
    sleep(1);

    // 连接各节点
    Node::connectToPeers({&s0, &s1, &s2, &o, &t});
    std::cout << "all servers have connected to peers" << std::endl;

    Protocol protocol({&s0, &s1, &s2}, &t);

    // 读取表
    Table match_result(Table::INT_MODE);
    try {
        match_result.readFromFile(PROJECT_PATH + "/data/match_result_124.csv");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    // 生成表的附带信息
    // 定义查询图中对称节点组，索引从0开始
    std::vector<std::vector<int>> symmetries = {{2,4}, {1,3}};

    // Owner把数据图共享给Servers
    o.setVar("M", match_result);
    o.shareTable("M", {BINARY_SHARING, BINARY_SHARING, BINARY_SHARING}, {&s0, &s1, &s2});

    protocol.checkMatch("[M]", symmetries);

    t.revealTable("[M]");
    t.printVars();

    // 主动关闭所有节点，先关闭依赖其他节点的对象
    o.stop();
    t.stop();
    s0.stop();
    s1.stop();
    s2.stop();

    // Clean up threads
    t0.join();
    t1.join();
    t2.join();
    to.join();
    tt.join();
}

void star_decomposition_test() {
    // 示例输入
    // std::vector<std::vector<int>> edges = {
    //     {0, 1}, {0, 2}, {1, 2}, {2, 3}, {2, 4}, {3, 4}, {4, 5}, {4, 6}
    // };

    std::vector<std::vector<int>> edges = {
        {1, 2}, {1, 4}, {2, 3}, {2, 4}, {3, 4}
    };
    
    std::cout << "原始图的边：" << std::endl;
    for (const auto& edge : edges) {
        std::cout << edge[0] << " -- " << edge[1] << std::endl;
    }
    
    std::vector<Utils::StarGraph> decomposition = Utils::starDecomposition(edges);
    
    std::cout << "\n星型分解结果(" << decomposition.size() << "个星型子图): " << std::endl;
    for (size_t i = 0; i < decomposition.size(); i++) {
        std::cout << "星型子图 " << i + 1 << ": ";
        decomposition[i].print();
    }
    
}

void limit_test() {
    std::cout << "int 的最小值为: " << std::numeric_limits<int>::min() << std::endl;
    std::cout << "int 的最大值为: " << std::numeric_limits<int>::max() << std::endl;
}

void mix_upper_bound_test() {
    std::vector<std::vector<int>> edges = {{1, 3}, {1, 4}, {2, 4}, {2, 5}, {3, 5}};

    auto bound = Utils::mixedUpperBound(edges, 2834, 12, {{1, 2, 3, 4, 5}});
    std::cout << bound << std::endl;
}

void hash_join_test() {
    // // 创建节点对象
    // Server s0(0, "SERVER 0");
    // Server s1(1, "SERVER 1");
    // Server s2(2, "SERVER 2");
    // Node o(3, "OWNER");
    // ThirdParty t(4, "THIRDPARTY", {&s0, &s1, &s2});
    
    // //为节点对象创建线程，开始监听
    // std::thread t0(&Node::startlistening, &s0);
    // std::thread t1(&Node::startlistening, &s1);
    // std::thread t2(&Node::startlistening, &s2);
    // std::thread to(&Node::startlistening, &o);
    // std::thread tt(&Node::startlistening, &t);
    // std::cout << "all servers are listening" << std::endl;
    // sleep(1);

    // // 连接各节点
    // Node::connectToPeers({&s0, &s1, &s2, &o, &t});
    // std::cout << "all servers have connected to peers" << std::endl;

    // Protocol protocol({&s0, &s1, &s2}, &t);

    // 读取表
    Table t1(Table::INT_MODE);
    Table t2(Table::INT_MODE);
    try {
        t1.readFromFile(PROJECT_PATH + "/data/table1_data.csv");
        t2.readFromFile(PROJECT_PATH + "/data/table2_data.csv");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    std::cout << "T1" << std::endl << t1.toString() << std::endl;
    std::cout << "T2" << std::endl << t2.toString() << std::endl;

    // Owner把共享给Servers
    // o.setVar("T1", t1);
    // o.setVar("T2", t2);
    // o.shareTable("T1", BINARY_SHARING, {&s0, &s1, &s2});
    // o.shareTable("T2", BINARY_SHARING, {&s0, &s1, &s2});

    Table t3 = Join_Hash(t1, t2);
    std::cout << t3.toString() << std::endl;

}

void example_test() {
    // 创建节点对象
    Server s0(0, "SERVER 0");
    Server s1(1, "SERVER 1");
    Server s2(2, "SERVER 2");
    Node o(3, "OWNER");
    ThirdParty t(4, "THIRDPARTY", {&s0, &s1, &s2});
    
    //为节点对象创建线程，开始监听
    std::thread t0(&Node::startlistening, &s0);
    std::thread t1(&Node::startlistening, &s1);
    std::thread t2(&Node::startlistening, &s2);
    std::thread to(&Node::startlistening, &o);
    std::thread tt(&Node::startlistening, &t);
    std::cout << "all servers are listening" << std::endl;
    sleep(1);

    // 连接各节点
    Node::connectToPeers({&s0, &s1, &s2, &o, &t});
    std::cout << "all servers have connected to peers" << std::endl;

    Protocol protocol({&s0, &s1, &s2}, &t);

    // 读取表
    Table data_graph(Table::INT_MODE);
    Table query_graph(Table::INT_MODE);
    try {
        data_graph.readFromFile(PROJECT_PATH + "/data/data_graph/data_example_SWW12.csv");
        query_graph.readFromFile(PROJECT_PATH + "/data/query_graph/query_triangle.csv");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
    std::cout << "Query Graph:\n" << query_graph.toString() << std::endl;

    // 生成表的附带信息
    auto edges = query_graph.selectAsInt(query_graph.headers);

    // 构造data_graph的DP索引（为了直观的测试Join，此处的索引是不加噪的）
    std::vector<Range> ranges = data_graph.getAllColumnValueRange();
    std::vector<int> widths(data_graph.headers.size(), (ranges[0].second - ranges[0].first + 1) / 3);
    auto records = data_graph.selectAsInt(data_graph.headers);
    Histogram hst_uv(data_graph.headers, records, ranges);
    Histogram hst_u = hst_uv.reduction({hst_uv.getAttrs()[0]});
    Histogram hst_v = hst_uv.reduction({hst_uv.getAttrs()[1]});
    Index idx_uv(hst_uv, widths, false);
    Index idx_u(hst_u, {widths[0]}, false);
    Index idx_v(hst_v, {widths[1]}, false);

    // Owner把数据图共享给Servers
    o.setVar("G", data_graph);
    o.shareTable("G", {BINARY_SHARING, BINARY_SHARING}, {&s0, &s1, &s2});

    // 给Servers设置data_graph的DP-Index
    s0.setTableIndex("[G]", idx_uv);
    s0.setTableIndex("[G]", idx_u);
    s0.setTableIndex("[G]", idx_v);
    s1.setTableIndex("[G]", idx_uv);
    s1.setTableIndex("[G]", idx_u);
    s1.setTableIndex("[G]", idx_v);
    s2.setTableIndex("[G]", idx_uv);
    s2.setTableIndex("[G]", idx_u);
    s2.setTableIndex("[G]", idx_v);

    // Servers开始执行子图匹配协议，结果表名为[PR]
    protocol.use_third_party = true;
    protocol.subgraphMatching(query_graph, "[PR]");

    t.revealTable("[PR]");
    auto& result = std::any_cast<Table&>(t.getVar("PR"));
    result.normalize();
    t.printVarsInFile();

    // 主动关闭所有节点，先关闭依赖其他节点的对象
    o.stop();
    t.stop();
    s0.stop();
    s1.stop();
    s2.stop();

    // Clean up threads
    t0.join();
    t1.join();
    t2.join();
    to.join();
    tt.join();
}

void GRQC_test() {
    // 创建节点对象
    Server s0(0, "SERVER 0");
    Server s1(1, "SERVER 1");
    Server s2(2, "SERVER 2");
    Node o(3, "OWNER");
    ThirdParty t(4, "THIRDPARTY", {&s0, &s1, &s2});
    
    //为节点对象创建线程，开始监听
    std::thread t0(&Node::startlistening, &s0);
    std::thread t1(&Node::startlistening, &s1);
    std::thread t2(&Node::startlistening, &s2);
    std::thread to(&Node::startlistening, &o);
    std::thread tt(&Node::startlistening, &t);
    std::cout << "all servers are listening" << std::endl;
    sleep(1);

    // 连接各节点
    Node::connectToPeers({&s0, &s1, &s2, &o, &t});
    std::cout << "all servers have connected to peers" << std::endl;

    Protocol protocol({&s0, &s1, &s2}, &t);

    // 读取表
    Table data_graph(Table::INT_MODE);
    Table query_graph(Table::INT_MODE);
    try {
        data_graph.readFromFile(PROJECT_PATH + "/data/data_graph/data_GR-QC.csv");
        query_graph.readFromFile(PROJECT_PATH + "/data/query_graph/query_triangle.csv");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
    std::cout << "Query Graph:\n" << query_graph.toString() << std::endl;

    // 生成表的附带信息
    auto edges = query_graph.selectAsInt(query_graph.headers);

    // 构造data_graph的DP索引（为了直观的测试Join，此处的索引是不加噪的）
    std::vector<Range> ranges = data_graph.getAllColumnValueRange();
    std::vector<int> widths(data_graph.headers.size(), (ranges[0].second - ranges[0].first + 1) / 10);
    auto records = data_graph.selectAsInt(data_graph.headers);
    Histogram hst_uv(data_graph.headers, records, ranges);
    Histogram hst_u = hst_uv.reduction({hst_uv.getAttrs()[0]});
    Histogram hst_v = hst_uv.reduction({hst_uv.getAttrs()[1]});
    Index idx_uv(hst_uv, widths, false);
    Index idx_u(hst_u, {widths[0]}, false);
    Index idx_v(hst_v, {widths[1]}, false);

    // Owner把数据图共享给Servers
    o.setVar("G", data_graph);
    o.shareTable("G", {BINARY_SHARING, BINARY_SHARING}, {&s0, &s1, &s2});

    // 给Servers设置data_graph的DP-Index
    s0.setTableIndex("[G]", idx_uv);
    s0.setTableIndex("[G]", idx_u);
    s0.setTableIndex("[G]", idx_v);
    s1.setTableIndex("[G]", idx_uv);
    s1.setTableIndex("[G]", idx_u);
    s1.setTableIndex("[G]", idx_v);
    s2.setTableIndex("[G]", idx_uv);
    s2.setTableIndex("[G]", idx_u);
    s2.setTableIndex("[G]", idx_v);

    // Servers开始执行子图匹配协议，结果表名为[PR]
    protocol.use_third_party = true;
    protocol.subgraphMatching(query_graph, "[PR]");

    t.revealTable("[PR]");
    auto& result = std::any_cast<Table&>(t.getVar("PR"));
    result.normalize();
    t.printVarsInFile();

    // 主动关闭所有节点，先关闭依赖其他节点的对象
    o.stop();
    t.stop();
    s0.stop();
    s1.stop();
    s2.stop();

    // Clean up threads
    t0.join();
    t1.join();
    t2.join();
    to.join();
    tt.join();
}

void Euroroads_test() {
    // 创建节点对象
    Server s0(0, "SERVER 0");
    Server s1(1, "SERVER 1");
    Server s2(2, "SERVER 2");
    Node o(3, "OWNER");
    ThirdParty t(4, "THIRDPARTY", {&s0, &s1, &s2});
    
    //为节点对象创建线程，开始监听
    std::thread t0(&Node::startlistening, &s0);
    std::thread t1(&Node::startlistening, &s1);
    std::thread t2(&Node::startlistening, &s2);
    std::thread to(&Node::startlistening, &o);
    std::thread tt(&Node::startlistening, &t);
    std::cout << "all servers are listening" << std::endl;
    sleep(1);

    // 连接各节点
    Node::connectToPeers({&s0, &s1, &s2, &o, &t});
    std::cout << "all servers have connected to peers" << std::endl;

    Protocol protocol({&s0, &s1, &s2}, &t);

    // 读取表
    Table data_graph(Table::INT_MODE);
    Table query_graph(Table::INT_MODE);
    try {
        data_graph.readFromFile(PROJECT_PATH + "/data/data_graph/data_Euroroads.csv");
        query_graph.readFromFile(PROJECT_PATH + "/data/query_graph/query_triangle.csv");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
    std::cout << "Query Graph:\n" << query_graph.toString() << std::endl;

    // 生成表的附带信息
    auto edges = query_graph.selectAsInt(query_graph.headers);

    // 构造data_graph的DP索引（为了直观的测试Join，此处的索引是不加噪的）
    std::vector<Range> ranges = data_graph.getAllColumnValueRange();
    std::vector<int> widths(data_graph.headers.size(), (ranges[0].second - ranges[0].first + 1) / 10);
    auto records = data_graph.selectAsInt(data_graph.headers);
    Histogram hst_uv(data_graph.headers, records, ranges);
    Histogram hst_u = hst_uv.reduction({hst_uv.getAttrs()[0]});
    Histogram hst_v = hst_uv.reduction({hst_uv.getAttrs()[1]});
    Index idx_uv(hst_uv, widths, false);
    Index idx_u(hst_u, {widths[0]}, false);
    Index idx_v(hst_v, {widths[1]}, false);

    // Owner把数据图共享给Servers
    o.setVar("G", data_graph);
    o.shareTable("G", {BINARY_SHARING, BINARY_SHARING}, {&s0, &s1, &s2});

    // 给Servers设置data_graph的DP-Index
    s0.setTableIndex("[G]", idx_uv);
    s0.setTableIndex("[G]", idx_u);
    s0.setTableIndex("[G]", idx_v);
    s1.setTableIndex("[G]", idx_uv);
    s1.setTableIndex("[G]", idx_u);
    s1.setTableIndex("[G]", idx_v);
    s2.setTableIndex("[G]", idx_uv);
    s2.setTableIndex("[G]", idx_u);
    s2.setTableIndex("[G]", idx_v);

    // Servers开始执行子图匹配协议，结果表名为[PR]
    protocol.use_third_party = false;
    protocol.subgraphMatching(query_graph, "[PR]");

    t.revealTable("[PR]");
    auto& result = std::any_cast<Table&>(t.getVar("PR"));
    result.normalize();
    t.printVarsInFile();

    // 主动关闭所有节点，先关闭依赖其他节点的对象
    o.stop();
    t.stop();
    s0.stop();
    s1.stop();
    s2.stop();

    // Clean up threads
    t0.join();
    t1.join();
    t2.join();
    to.join();
    tt.join();
}

void end_to_end_test(const std::string& G_file_name, const std::string& Q_file_name, bool use_hierarchy, bool use_third_party, bool use_join_strategy, bool add_noise, double e_idx, double e_mf) {
    // 创建节点对象
    Server s0(0, "SERVER 0");
    Server s1(1, "SERVER 1");
    Server s2(2, "SERVER 2");
    Node o(3, "OWNER");
    ThirdParty t(4, "THIRDPARTY", {&s0, &s1, &s2});
    
    //为节点对象创建线程，开始监听
    std::thread t0(&Node::startlistening, &s0);
    std::thread t1(&Node::startlistening, &s1);
    std::thread t2(&Node::startlistening, &s2);
    std::thread to(&Node::startlistening, &o);
    std::thread tt(&Node::startlistening, &t);
    // std::cout << "all servers are listening" << std::endl;
    // sleep(1);

    // 连接各节点
    Node::connectToPeers({&s0, &s1, &s2, &o, &t});
    // std::cout << "all servers have connected to peers" << std::endl;

    Protocol protocol({&s0, &s1, &s2}, &t);

    // 读取表
    Table data_graph(Table::INT_MODE);
    Table query_graph(Table::INT_MODE);
    try {
        data_graph.readFromFile(PROJECT_PATH + "/data/data_graph/" + G_file_name + ".csv");
        query_graph.readFromFile(PROJECT_PATH + "/data/query_graph/" + Q_file_name + ".csv");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
    // std::cout << "Query Graph:\n" << query_graph.toString() << std::endl;

    // 构造data_graph的DP索引（为了直观的测试Join，此处的索引是不加噪的）
    std::vector<Range> ranges = data_graph.getAllColumnValueRange();
    std::vector<int> widths(data_graph.headers.size(), std::ceil((ranges[0].second - ranges[0].first + 1) / double(10)));
    auto records = data_graph.selectAsInt(data_graph.headers);
    Histogram hst_uv(data_graph.headers, records, ranges);
    Histogram hst_u = hst_uv.reduction({hst_uv.getAttrs()[0]});
    Histogram hst_v = hst_uv.reduction({hst_uv.getAttrs()[1]});
    // Index idx_uv(hst_uv, widths, false);
    // Index idx_u(hst_u, {widths[0]}, add_noise, e_idx);
    // Index idx_v(hst_v, {widths[1]}, add_noise, e_idx);

    Index* idx_u;
    Index* idx_v;

    if (use_hierarchy) {
        HierarchicalHistogram hh(hst_u.getCounts(), hst_u.getRanges()[0]);
        HierarchicalHistogram hh_p = hh.addDPNoise(e_idx, 1);
        HierarchicalHistogram hh_n = hh.addDPNoise(e_idx, -1);
        idx_u = new Index(data_graph.headers[0], hh_p, hh_n);
        idx_v = new Index(data_graph.headers[1], hh_p, hh_n);
    } else {
        Histogram h_p = hst_u.addDPNoise(e_idx, 1);
        Histogram h_n = hst_u.addDPNoise(e_idx, -1);
        idx_u = new Index(h_p, h_n);
        idx_v = new Index(h_p, h_n);
    }

    // 根据 e_mf 对 mf 统计值加噪
    if (add_noise) {
        data_graph.epsilon = e_mf;
        data_graph.addNoiseToMFs();
    }

    // Owner把数据图共享给Servers
    o.setVar("G", data_graph);
    o.shareTable("G", {BINARY_SHARING, BINARY_SHARING}, {&s0, &s1, &s2});

    // 给Servers设置data_graph的DP-Index
    s0.setTableIndex("[G]", *idx_u);
    s0.setTableIndex("[G]", *idx_v);
    s1.setTableIndex("[G]", *idx_u);
    s1.setTableIndex("[G]", *idx_v);
    s2.setTableIndex("[G]", *idx_u);
    s2.setTableIndex("[G]", *idx_v);

    // Servers开始执行子图匹配协议，结果表名为[PR]
    protocol.use_third_party = use_third_party;
    if (use_join_strategy) {
        protocol.subgraphMatching(query_graph, "[PR]");
    } else {
        protocol.subgraphMatchingNoJoinPlan(query_graph, "[PR]");
        // protocol.subgraphMatchingOnlyMPC(query_graph, "[PR]");
    }
    
    t.revealTable("[PR]");
    auto& result = std::any_cast<Table&>(t.getVar("PR"));
    result.normalize();
    t.printVarsInFile();

    // 主动关闭所有节点，先关闭依赖其他节点的对象
    o.stop();
    t.stop();
    s0.stop();
    s1.stop();
    s2.stop();

    // Clean up threads
    t0.join();
    t1.join();
    t2.join();
    to.join();
    tt.join();
}