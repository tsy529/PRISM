#include "Function.h"

namespace Utils {

// 并查集（DSU）用于合并轨道
struct DSU {
    std::vector<int> parent;
    DSU(int n) : parent(n) {
        iota(parent.begin(), parent.end(), 0);
    }
    int find(int x) {
        if (parent[x] != x) {
            parent[x] = find(parent[x]);
        }
        return parent[x];
    }
    void unite(int x, int y) {
        int fx = find(x), fy = find(y);
        if (fx != fy) {
            parent[fy] = fx;
        }
    }
};

std::vector<std::vector<int>> getSymmetries(std::vector<std::vector<int>> edges) {
    // 确定节点数n
    int n = 0;
    for (auto& edge : edges) {
        for (int v : edge) {
            n = std::max(n, v + 1);
        }
    }
    if (n == 0) return {};

    // 构建邻接矩阵（无向图）
    std::vector<std::vector<bool>> adj(n, std::vector<bool>(n, false));
    for (auto& edge : edges) {
        int u = edge[0], v = edge[1];
        adj[u][v] = adj[v][u] = true;
    }

    // 生成所有排列并筛选自同构
    std::vector<std::vector<int>> aut_list;
    std::vector<int> perm(n);
    std::iota(perm.begin(), perm.end(), 0);
    do {
        bool is_aut = true;
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < n; ++j) {
                if (adj[i][j] != adj[perm[i]][perm[j]]) {
                    is_aut = false;
                    goto end_check;
                }
            }
        }
    end_check:
        if (is_aut) {
            aut_list.push_back(perm);
        }
    } while (next_permutation(perm.begin(), perm.end()));

    // 合并轨道
    DSU dsu(n);
    for (auto& p : aut_list) {
        for (int v = 0; v < n; ++v) {
            dsu.unite(v, p[v]);
        }
    }

    // 收集轨道结果
    std::unordered_map<int, std::vector<int>> groups;
    for (int v = 0; v < n; ++v) {
        groups[dsu.find(v)].push_back(v);
    }

    std::vector<std::vector<int>> result;
    for (auto& [root, nodes] : groups) {
        if (nodes.size() <= 1) {
            continue;
        }
        sort(nodes.begin(), nodes.end());
        result.push_back(nodes);
    }

    // 按轨道首元素排序
    sort(result.begin(), result.end(), [](const std::vector<int>& a, const std::vector<int>& b) {
        return a[0] < b[0];
    });

    return result;
}

std::vector<std::vector<int>> getSymmetries(std::vector<std::vector<int>> edges, std::ostream &os) {
    auto result = getSymmetries(edges);

    // 打印结果
    os << "Symmetries: { ";
    for (auto &group : result) {
        os << "{";
        for (size_t i = 0; i < group.size(); ++i) {
            os << group[i] << (i < group.size()-1 ? ", " : "");
        }
        os << "} ";
    }
    os << "}";

    return result;
}

// 图星型分解的函数
std::vector<StarGraph> starDecomposition(std::vector<std::vector<int>> edges) {
    std::vector<StarGraph> result;
    if (edges.empty()) return result;
    
    // 构建邻接表
    std::unordered_map<int, std::vector<int>> adjList;
    for (const auto& edge : edges) {
        adjList[edge[0]].push_back(edge[1]);
        adjList[edge[1]].push_back(edge[0]);
    }
    
    // 循环直到所有边都被处理
    while (!edges.empty()) {
        // 计算每个节点的度
        std::unordered_map<int, int> degrees;
        for (const auto& edge : edges) {
            degrees[edge[0]]++;
            degrees[edge[1]]++;
        }
        
        // 找到度之和最高的边
        int maxDegreeSum = -1;
        int maxDegreeNode = -1;
        int maxDegree = -1;
        
        for (const auto& [node, degree] : degrees) {
            if (degree > maxDegree) {
                maxDegree = degree;
                maxDegreeNode = node;
            }
        }
        
        // 创建以最大度节点为中心的星型子图
        StarGraph star;
        star.root = maxDegreeNode;
        
        // 找出与中心节点相连的所有边并记录邻居
        std::vector<std::vector<int>> remainingEdges;
        for (const auto& edge : edges) {
            if (edge[0] == maxDegreeNode) {
                star.neighbors.push_back(edge[1]);
            } else if (edge[1] == maxDegreeNode) {
                star.neighbors.push_back(edge[0]);
            } else {
                remainingEdges.push_back(edge);
            }
        }
        
        // 更新边集
        edges = remainingEdges;
        
        // 添加星型子图到结果中
        result.push_back(star);
    }
    
    return result;
}

std::vector<StarGraph> starDecomposition(std::vector<std::vector<int>> edges, std::ostream &os) {
    auto stars = starDecomposition(edges);

    // 输出打印信息
    os << "Star decompositon results (" << stars.size() << " stars): ";
    for (size_t i = 0; i < stars.size(); i++) {
        os << "\n\tstar " << i + 1 << " => ";
        stars[i].print(os);
    }

    return stars;
}

StarGraph& findbiggestStar(std::vector<StarGraph>& stars) {
    auto maxStarIt = std::max_element(stars.begin(), stars.end(), [](const StarGraph& a, const StarGraph& b) {
        return a.size() < b.size(); 
    });

    return *maxStarIt;
}

// 星型子图平衡处理
std::vector<StarGraph> balanceStars(std::vector<StarGraph> stars) {
    while(true) {
        // 用来记录本轮处理是否降低了biggest的size
        bool reduced = false;
        // 找出当前最大的star
        auto& biggest = findbiggestStar(stars);
        // 尝试将biggest的边分摊到其他较小的star上
        for (auto v : biggest.neighbors) {
            // 如果v是较小star的root，且移动该边能降低整体最大star的size（size相差大于1）
            for (auto& smaller : stars) {
                if (smaller.root == v && biggest.size() - smaller.size() > 1) {
                    // 移动边至smaller
                    biggest.deleteNeighbor(v);
                    smaller.addNeighbor(biggest.root);
                    reduced = true;
                }
            }
        }
        if (!reduced) {
            break;
        }
    }

    return stars;
}

std::vector<StarGraph> balanceStars(std::vector<StarGraph> stars, std::ostream &os) {
    auto stars_b = balanceStars(stars);

    // 输出打印信息
    os << "Star balance results (" << stars_b.size() << " stars): ";
    for (size_t i = 0; i < stars_b.size(); i++) {
        os << "\n\tstar " << i + 1 << " => ";
        stars_b[i].print(os);
    }

    return stars_b;
}

int countStarGraphsOfSize(const std::vector<StarGraph>& stars, int size) {
    int count = 0;
    
    for (const auto& star : stars) {
        // 检查星型子图的邻居数量是否等于size
        if (star.neighbors.size() == size) {
            count++;
        }
    }
    
    return count;
}

uint64_t computeMFBound(std::vector<uint64_t> MF, std::vector<uint64_t> N) {
    // 参数校验
    if (MF.empty() || N.empty()) {
        throw std::invalid_argument("Input vectors cannot be empty");
    }
    if (MF.size() != N.size()) {
        throw std::invalid_argument("MF and N must have the same size");
    }
    if (std::any_of(MF.begin(), MF.end(), [](uint64_t mf) { return mf == 0; })) {
        throw std::invalid_argument("All MF values must be greater than 0");
    }

    double min_part = static_cast<double>(N[0]) / static_cast<double>(MF[0]);
    uint64_t mul_part = MF[0];
    for (int i = 1; i < MF.size(); i++) {
        min_part = std::min(min_part, static_cast<double>(N[i]) / static_cast<double>(MF[i]));
        mul_part *= MF[i];
    }

    uint64_t bound = std::ceil(min_part * mul_part);
    return bound;
}

uint64_t computeMFBound(std::vector<std::vector<uint64_t>> MFs, std::vector<uint64_t> N) {
    // 针对每个连接键计算上界，选择最小的作为总体上界
    uint64_t min_bound = UINT64_MAX;
    for (int i = 0; i < MFs.size(); i++) {
        uint64_t bound = computeMFBound(MFs[i], N);
        if (bound < min_bound) {
            min_bound = bound;
        }
    }
    return min_bound;
}

uint64_t stepwiseMFBound(std::vector<uint64_t> MF, std::vector<uint64_t> N) {
    if (N.size() <= 1) return N[0];

    // 每次计算前两个表的MF bound
    std::vector<uint64_t> current_MF = {MF[0], MF[1]};
    std::vector<uint64_t> current_N = {N[0], N[1]};
    uint64_t current_bound = computeMFBound(current_MF, current_N);
    std::cout << "MF step bound: " << current_bound << std::endl;

    // 生成新的参数
    std::vector<uint64_t> remaining_MF;
    std::vector<uint64_t> remaining_N;
    // 连接后最大值变化按照论文中方式计算（求乘积）
    remaining_MF.push_back(MF[0] * MF[1]);
    remaining_N.push_back(current_bound);
    for (int i = 2; i < N.size(); i++) {
        remaining_MF.push_back(MF[i]);
        remaining_N.push_back(N[i]);
    }

    // 递归计算剩余表的上界
    return stepwiseMFBound(remaining_MF, remaining_N);
}

uint64_t computeAGMBound(std::set<std::string> V, std::vector<std::set<std::string>> E, std::vector<uint64_t> N) {
    glp_term_out(GLP_OFF);

    // 初始化线性规划问题
    glp_prob *lp = glp_create_prob();
    glp_set_obj_dir(lp, GLP_MIN);       // 最小化目标函数

    // 添加变量：每条边对应一个变量x_e（索引从1开始）
    int num_edges = E.size();
    glp_add_cols(lp, num_edges);
    for (int e = 0; e < num_edges; e++) {
        glp_set_col_bnds(lp, e + 1, GLP_LO, 0.0, 0.0);  // x_e >= 0
        glp_set_obj_coef(lp, e + 1, std::log(N[e]));    // 目标函数系数为 log(N_e)
    }

    // 添加约束：每个属性 v 的覆盖约束
    int constraint_id = 0;
    for (const auto& v : V) {
        glp_add_rows(lp, 1);        // 为每个属性添加一行约束
        constraint_id++;

        // 找到所有包含属性 v 的边
        std::vector<int> edges_with_v;
        for (int e = 0; e < num_edges; e++) {
            if (E[e].find(v) != E[e].end()) {
                edges_with_v.push_back(e + 1);      // GLPK 变量索引从1开始
            }
        }

        // 设置约束系数：sum_{v in e} x_e >= 1
        int num_coeff = edges_with_v.size();
        std::vector<int> indices(num_coeff + 1);
        std::vector<double> coeffs(num_coeff + 1, 1.0);
        for (int i = 0; i < num_coeff; i++) {
            indices[i + 1] = edges_with_v[i];
        }

        glp_set_row_bnds(lp, constraint_id, GLP_LO, 1.0, 0.0);
        glp_set_mat_row(lp, constraint_id, num_coeff, indices.data(), coeffs.data());
    }

    // 求解线性规划
    glp_simplex(lp, nullptr);

    // 计算上界: exp(最优目标值)
    double obj_val = glp_get_obj_val(lp);
    uint64_t bound = static_cast<uint64_t>(std::exp(obj_val) + 0.5);  // 四舍五入

    // 打印分数覆盖结果
    // std::cout << "Optimal Fractional Edge Cover:" << std::endl;
    // for (int e = 0; e < num_edges; e++) {
    //     double x_e = glp_get_col_prim(lp, e + 1);
    //     std::cout << "Edge " << e << ": x_e = " << x_e << std::endl;
    // }

    // 清理资源
    glp_delete_prob(lp);
    return bound;
}

uint64_t computeAGMBound(std::vector<std::string> t1_headers, std::vector<std::string> t2_headers, uint64_t t1_size, uint64_t t2_size) {
    // 初始化超图点集、边集
    std::set<std::string> V;
    std::vector<std::set<std::string>> E;
    std::vector<uint64_t> N;

    // 加入 t1 的信息
    for (const auto& header : t1_headers) {
        V.insert(header);
    }
    E.push_back(std::set<std::string>(t1_headers.begin(), t1_headers.end()));
    N.push_back(t1_size);

    // 加入 t2 的信息
    for (const auto& header : t2_headers) {
        V.insert(header);
    }
    E.push_back(std::set<std::string>(t2_headers.begin(), t2_headers.end()));
    N.push_back(t2_size);

    return computeAGMBound(V, E, N);
}

uint64_t computeAGMBound(const std::vector<std::vector<int>>& edges, uint64_t size) {
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

    return computeAGMBound(V, E, N);
}

uint64_t computeStarAGMBound(u_int star_size, uint64_t size) {
    std::set<std::string> V;
    std::vector<std::set<std::string>> E;
    std::vector<uint64_t> N;

    // 构造大小为star_size的星型拓扑结构
    for (int i = 1; i <= star_size; i++) {
        auto u = std::to_string(0);
        auto v = std::to_string(i);
        V.insert(u);
        V.insert(v);
        E.push_back(std::set<std::string>({u, v}));
        N.push_back(size);
    }

    return Utils::computeAGMBound(V, E, N);
}

uint64_t stepwiseAGMBound(std::set<std::string> V, std::vector<std::set<std::string>> E, std::vector<uint64_t> N) {
    if (E.size() <= 1) return N[0];

    // 每次合并前两张表
    auto merged_V = E[0];
    merged_V.insert(E[1].begin(), E[1].end());

    // 计算合并后的上界
    std::vector<std::set<std::string>> new_E = {E[0], E[1]};
    std::vector<uint64_t> new_N = {N[0], N[1]};
    uint64_t merged_size = computeAGMBound(merged_V, new_E, new_N);

    std::cout << "AGM step bound: " << merged_size << std::endl;

    // 生成新的表集合
    std::vector<std::set<std::string>> remaining_E;
    std::vector<uint64_t> remaining_N;
    remaining_E.push_back(merged_V);
    remaining_N.push_back(merged_size);
    for (size_t i = 2; i < E.size(); i++) {
        remaining_E.push_back(E[i]);
        remaining_N.push_back(N[i]);
    }

    // 递归计算剩余表的上界
    return stepwiseAGMBound(merged_V, remaining_E, remaining_N);
}

// ----------------- 计算混合上界 -----------------

// Join Tree节点结构
struct TreeNode {
    bool isLeaf;
    std::vector<int> edge; // 对于叶子节点，存储边的两个顶点
    std::shared_ptr<TreeNode> left;
    std::shared_ptr<TreeNode> right;
    std::set<int> attributes;                     // 该节点包含的所有属性
    uint64_t bound;                                    // 上界估计
    uint64_t mf_value;                                 // mf值
    std::vector<std::vector<int>> subgraph_edges; // 该子树对应的所有边

    // 新增：记录选择的上界类型和各个上界值
    std::string boundType;  // "MF", "AGM", "SW"
    uint64_t mfBoundValue;
    uint64_t agmBoundValue;
    uint64_t swBoundValue;
    std::set<int> joinKeys;  // 记录连接键
};

// 对称性处理相关结构
class SymmetryHandler {
private:
    std::map<int, int> nodeToGroup;  // 节点到对称组的映射
    std::vector<std::vector<int>> symmetryGroups;  // 对称组
    
public:
    SymmetryHandler(const std::vector<std::vector<int>>& symmetries) {
        int groupId = 0;
        for (const auto& group : symmetries) {
            if (group.empty()) continue;
            
            symmetryGroups.push_back(group);
            
            for (int node : group) {
                nodeToGroup[node] = groupId;
            }
            groupId++;
        }
    }
    
    // 判断两个节点是否对称
    bool areSymmetric(int node1, int node2) const {
        if (node1 == node2) return true;
        
        auto it1 = nodeToGroup.find(node1);
        auto it2 = nodeToGroup.find(node2);
        
        if (it1 == nodeToGroup.end() || it2 == nodeToGroup.end()) {
            return false;
        }
        
        return it1->second == it2->second;
    }
    
    // 计算边集合的规范化签名（用于判断等价性）
    std::string computeCanonicalSignature(const std::vector<std::vector<int>>& edges) const {
        // 构建邻接表
        std::map<int, std::set<int>> adj;
        for (const auto& edge : edges) {
            adj[edge[0]].insert(edge[1]);
            adj[edge[1]].insert(edge[0]);
        }
        
        // 为每个节点计算度数签名
        std::map<int, std::vector<int>> nodeSignatures;
        for (const auto& [node, neighbors] : adj) {
            std::vector<int> signature;
            
            // 统计邻居在各个对称组中的分布
            std::map<int, int> groupCount;
            for (int neighbor : neighbors) {
                auto it = nodeToGroup.find(neighbor);
                int groupId = (it != nodeToGroup.end()) ? it->second : -neighbor-1;
                groupCount[groupId]++;
            }
            
            // 构建签名
            for (const auto& [gid, count] : groupCount) {
                signature.push_back(gid);
                signature.push_back(count);
            }
            
            std::sort(signature.begin(), signature.end());
            nodeSignatures[node] = signature;
        }
        
        // 按对称组分组节点
        std::map<int, std::vector<std::pair<int, std::vector<int>>>> groupedNodes;
        for (const auto& [node, sig] : nodeSignatures) {
            auto it = nodeToGroup.find(node);
            int groupId = (it != nodeToGroup.end()) ? it->second : -node-1;
            groupedNodes[groupId].push_back({node, sig});
        }
        
        // 构建规范化签名字符串
        std::stringstream ss;
        for (const auto& [groupId, nodes] : groupedNodes) {
            // 对同一对称组内的节点按签名排序
            auto sortedNodes = nodes;
            std::sort(sortedNodes.begin(), sortedNodes.end(), 
                     [](const auto& a, const auto& b) { return a.second < b.second; });
            
            ss << groupId << ":";
            for (const auto& [node, sig] : sortedNodes) {
                ss << "[";
                for (int s : sig) {
                    ss << s << ",";
                }
                ss << "]";
            }
            ss << ";";
        }
        
        return ss.str();
    }
    
    // 判断两个边集合是否等价
    bool areEquivalent(const std::vector<std::vector<int>>& edges1,
                      const std::vector<std::vector<int>>& edges2) const {
        if (edges1.size() != edges2.size()) return false;
        
        return computeCanonicalSignature(edges1) == computeCanonicalSignature(edges2);
    }
};

// 获取边集合中的所有顶点
std::set<int> getVertices(const std::vector<std::vector<int>> &edges) {
    std::set<int> vertices;
    for (const auto &edge : edges) {
        vertices.insert(edge[0]);
        vertices.insert(edge[1]);
    }
    return vertices;
}

// 生成所有非空子集（不包括全集和空集）
std::vector<std::vector<int>> generateSubsets(const std::vector<int> &items) {
    std::vector<std::vector<int>> subsets;
    int n = items.size();
    for (int mask = 1; mask < (1 << n) - 1; mask++) {
        std::vector<int> subset;
        for (int i = 0; i < n; i++) {
            if (mask & (1 << i)) {
                subset.push_back(items[i]);
            }
        }
        subsets.push_back(subset);
    }
    return subsets;
}

// 计算两个集合的交集
std::set<int> setIntersection(const std::set<int> &set1, const std::set<int> &set2) {
    std::set<int> result;
    std::set_intersection(set1.begin(), set1.end(), set2.begin(), set2.end(), std::inserter(result, result.begin()));
    return result;
}

// 将属性集合转换为字符串
std::string attributesToString(const std::set<int>& attrs) {
    std::stringstream ss;
    ss << "{";
    bool first = true;
    for (int attr : attrs) {
        if (!first) ss << ",";
        ss << std::to_string(attr);
        first = false;
    }
    ss << "}";
    return ss.str();
}

// 将边转换为字符串
std::string edgeToString(const std::vector<int>& edge) {
    return std::string("R(") + std::to_string(edge[0]) + "," + std::to_string(edge[1]) + ")";
}

// 打印Join Tree
void printJoinTree(std::shared_ptr<TreeNode> node, int depth = 0) {
    std::string indent(depth * 2, ' ');
    
    if (node->isLeaf) {
        std::cout << indent << "Leaf: " << edgeToString(node->edge) 
                  << " [bound=" << node->bound << ", mf=" << node->mf_value << "]" << std::endl;
    } else {
        std::cout << indent << "Internal Node: attrs=" << attributesToString(node->attributes)
                  << " [bound=" << node->bound << " (" << node->boundType << "), mf=" << node->mf_value << "]" << std::endl;
        std::cout << indent << "  MF=" << node->mfBoundValue 
                  << ", AGM=" << node->agmBoundValue 
                  << ", SW=" << node->swBoundValue << std::endl;
        
        std::cout << indent << "Left child:" << std::endl;
        printJoinTree(node->left, depth + 1);
        
        std::cout << indent << "Right child:" << std::endl;
        printJoinTree(node->right, depth + 1);
    }
}

// 计算节点的上界
uint64_t computeNodeBound(std::shared_ptr<TreeNode> node, uint64_t N, uint64_t mf) {
    if (node->isLeaf) {
        node->bound = N;
        node->mf_value = mf;
        node->boundType = "LEAF";
        return N;
    }

    // 递归计算子节点
    uint64_t leftBound = computeNodeBound(node->left, N, mf);
    uint64_t rightBound = computeNodeBound(node->right, N, mf);

    // 计算MF Bound
    uint64_t mfBound = computeMFBound({node->left->mf_value, node->right->mf_value}, {leftBound, rightBound});
    // uint64_t mfBound = std::min(leftBound / node->left->mf_value, rightBound / node->right->mf_value) *
    //               node->left->mf_value * node->right->mf_value;
    node->mfBoundValue = mfBound;

    // 计算Global AGM Bound
    uint64_t agmBound = computeAGMBound(node->subgraph_edges, N);
    node->agmBoundValue = agmBound;

    // 计算Stepwise AGM Bound
    std::vector<int> leftAttrs(node->left->attributes.begin(), node->left->attributes.end());
    std::vector<int> rightAttrs(node->right->attributes.begin(), node->right->attributes.end());
    std::vector<std::string> leftAttrs_str;
    std::vector<std::string> rightAttrs_str;
    leftAttrs_str.resize(leftAttrs.size());
    rightAttrs_str.resize(rightAttrs.size());
    std::transform(leftAttrs.begin(), leftAttrs.end(), leftAttrs_str.begin(), [](int n) {
        return std::to_string(n);
    });
    std::transform(rightAttrs.begin(), rightAttrs.end(), rightAttrs_str.begin(), [](int n) {
        return std::to_string(n);
    });
    uint64_t swBound = computeAGMBound(leftAttrs_str, rightAttrs_str, leftBound, rightBound);
    node->swBoundValue = swBound;

    // 取最小值并记录类型
    node->bound = mfBound;
    node->boundType = "MF";
    
    if (agmBound < node->bound) {
        node->bound = agmBound;
        node->boundType = "AGM";
    }
    
    if (swBound < node->bound) {
        node->bound = swBound;
        node->boundType = "SW";
    }

    // 更新mf值：左右子树mf值的乘积
    node->mf_value = node->left->mf_value * node->right->mf_value;

    return node->bound;
}

// 生成所有可能的Join Tree
void generateJoinTrees(const std::vector<std::vector<int>> &edges, std::vector<std::shared_ptr<TreeNode>> &trees,
                       const SymmetryHandler &symHandler) {
    int n = edges.size();
    
    if (n == 1) {
        auto node = std::make_shared<TreeNode>();
        node->isLeaf = true;
        node->edge = edges[0];
        node->attributes.insert(edges[0][0]);
        node->attributes.insert(edges[0][1]);
        node->subgraph_edges = edges;
        trees.push_back(node);
        return;
    }
    
    std::map<std::vector<std::vector<int>>, std::vector<std::shared_ptr<TreeNode>>> memo;
    std::set<std::string> processedSignatures;  // 记录已处理的规范化签名
    
    std::function<std::vector<std::shared_ptr<TreeNode>>(std::vector<int>)> buildTrees;
    buildTrees = [&](std::vector<int> edgeIndices) -> std::vector<std::shared_ptr<TreeNode>> {
        std::vector<std::vector<int>> currentEdges;
        for (int idx : edgeIndices) {
            currentEdges.push_back(edges[idx]);
        }
        
        // 计算当前边集合的规范化签名
        std::string signature = symHandler.computeCanonicalSignature(currentEdges);
        
        // 只在第一次遇到这个签名时处理
        bool isFirst = processedSignatures.insert(signature).second;
        if (!isFirst && currentEdges.size() > 1) {
            return {};  // 已处理过等价的边集合
        }
        
        if (memo.find(currentEdges) != memo.end()) {
            return memo[currentEdges];
        }
        
        std::vector<std::shared_ptr<TreeNode>> result;
        
        if (edgeIndices.size() == 1) {
            auto node = std::make_shared<TreeNode>();
            node->isLeaf = true;
            node->edge = edges[edgeIndices[0]];
            node->attributes.insert(node->edge[0]);
            node->attributes.insert(node->edge[1]);
            node->subgraph_edges = currentEdges;
            result.push_back(node);
            memo[currentEdges] = result;
            return result;
        }
        
        auto subsets = generateSubsets(edgeIndices);
        std::set<std::pair<std::string, std::string>> processedPairs;
        
        for (const auto& leftIndices : subsets) {
            std::vector<int> rightIndices;
            std::set<int> leftSet(leftIndices.begin(), leftIndices.end());
            
            for (int idx : edgeIndices) {
                if (leftSet.find(idx) == leftSet.end()) {
                    rightIndices.push_back(idx);
                }
            }
            
            if (rightIndices.empty()) continue;
            
            // 构造左右边集合
            std::vector<std::vector<int>> leftEdges, rightEdges;
            for (int idx : leftIndices) {
                leftEdges.push_back(edges[idx]);
            }
            for (int idx : rightIndices) {
                rightEdges.push_back(edges[idx]);
            }
            
            // 计算左右边集合的签名
            std::string leftSig = symHandler.computeCanonicalSignature(leftEdges);
            std::string rightSig = symHandler.computeCanonicalSignature(rightEdges);
            
            // 检查是否已处理过这个划分
            if (processedPairs.find({leftSig, rightSig}) != processedPairs.end() ||
                processedPairs.find({rightSig, leftSig}) != processedPairs.end()) {
                continue;
            }
            processedPairs.insert({leftSig, rightSig});
            
            auto leftTrees = buildTrees(leftIndices);
            auto rightTrees = buildTrees(rightIndices);
            
            for (auto& leftTree : leftTrees) {
                for (auto& rightTree : rightTrees) {
                    std::set<int> commonAttributes = setIntersection(leftTree->attributes, 
                                                                    rightTree->attributes);
                    
                    if (commonAttributes.empty()) {
                        continue;
                    }
                    
                    auto node = std::make_shared<TreeNode>();
                    node->isLeaf = false;
                    node->left = leftTree;
                    node->right = rightTree;
                    node->joinKeys = commonAttributes;
                    
                    node->attributes = leftTree->attributes;
                    node->attributes.insert(rightTree->attributes.begin(), 
                                          rightTree->attributes.end());
                    
                    node->subgraph_edges = leftTree->subgraph_edges;
                    node->subgraph_edges.insert(node->subgraph_edges.end(),
                                               rightTree->subgraph_edges.begin(),
                                               rightTree->subgraph_edges.end());
                    
                    result.push_back(node);
                }
            }
        }
        
        memo[currentEdges] = result;
        return result;
    };
    
    std::vector<int> allIndices;
    for (int i = 0; i < n; i++) {
        allIndices.push_back(i);
    }
    
    trees = buildTrees(allIndices);
}

uint64_t mixedUpperBound(std::vector<std::vector<int>> edges, uint64_t N, uint64_t mf, std::vector<std::vector<int>> symmetries) {
    // 创建对称性处理器
    SymmetryHandler symHandler(symmetries);

    // 生成所有可能的Join Tree
    std::vector<std::shared_ptr<TreeNode>> trees;
    generateJoinTrees(edges, trees, symHandler);
    
    if (trees.empty()) {
        std::cout << "No valid join trees found!" << std::endl;
        return std::numeric_limits<int>::max();
    }
    // std::cout << "Total valid join trees after symmetry pruning: " << trees.size() << std::endl;
    
    // 计算每个tree的根节点上界，找到最小值
    uint64_t minBound = std::numeric_limits<uint64_t>::max();
    std::shared_ptr<TreeNode> optimalTree = nullptr;
    
    for (auto& tree : trees) {
        int bound = computeNodeBound(tree, N, mf);
        if (bound < minBound) {
            minBound = bound;
            optimalTree = tree;
        }
    }
    
    // 打印最优的Join Tree
    // std::cout << "\n=== Optimal Join Tree ===" << std::endl;
    // std::cout << "Minimum bound: " << minBound << std::endl;
    // std::cout << "\nTree structure:" << std::endl;
    // printJoinTree(optimalTree);
    
    return minBound;
}

std::vector<int> getSortingPermutation(const std::vector<int>& vec) {
    std::vector<int> permutation(vec.size());
    
    // 生成初始索引 {0, 1, 2, ..., n-1}
    std::iota(permutation.begin(), permutation.end(), 0);
    
    // 按原始向量的值排序索引
    std::sort(permutation.begin(), permutation.end(),
              [&vec](int i, int j) { return vec[i] < vec[j]; });
    
    return permutation;
}

}

