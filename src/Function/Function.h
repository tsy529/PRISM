#pragma once

#include <iostream>
#include <sstream>
#include <vector>
#include <set>
#include <unordered_set>
#include <string>
#include <numeric>
#include <cstdint>
#include <algorithm>
#include <cmath>
#include <map>
#include <memory>
#include <glpk.h>

/// @brief 
namespace Utils {
struct DSU;

// 星型子图的表示形式：中心节点 + 邻接节点列表
struct StarGraph {
    int root;
    std::vector<int> neighbors;
    
    void print() const {
        std::cout << "Center: " << root << ", Neighbors: ";
        for (int neighbor : neighbors) {
            std::cout << neighbor << " ";
        }
        std::cout << std::endl;
    }

    void print(std::ostream &os) const {
        os << "Center: " << root << ", Neighbors: ";
        for (int neighbor : neighbors) {
            os << neighbor << " ";
        }
    }

    int size() const {
        return neighbors.size();
    }

    bool isNeighbor(int u) {
        auto it = std::find(neighbors.begin(), neighbors.end(), u);
        return it != neighbors.end();
    }

    void addNeighbor(int u) {
        auto it = std::lower_bound(neighbors.begin(), neighbors.end(), u);
        neighbors.insert(it, u);
    }
    
    void deleteNeighbor(int u) {
        auto it = std::find(neighbors.begin(), neighbors.end(), u);
        if (it != neighbors.end()) {
            neighbors.erase(it);
        }
    }

    std::vector<std::vector<int>> toEdges() const {
        std::vector<std::vector<int>> edges;
        for (int u : neighbors) {
            edges.push_back({root, u});
        }
        return edges;
    }

    std::vector<std::string> toStrings() const {
        std::vector<std::string> strs;
        strs.push_back(std::to_string(root));
        for (int u : neighbors) {
            strs.push_back(std::to_string(u));
        }
        return strs;
    }
};

// 计算对称节点
std::vector<std::vector<int>> getSymmetries(std::vector<std::vector<int>> edges);

// 计算对称节点, os存储打印信息
std::vector<std::vector<int>> getSymmetries(std::vector<std::vector<int>> edges, std::ostream &os);

// 图星型分解的函数
std::vector<StarGraph> starDecomposition(std::vector<std::vector<int>> edges);

// 图星型分解的函数，os存储打印信息
std::vector<StarGraph> starDecomposition(std::vector<std::vector<int>> edges, std::ostream &os);

// 从星型子图集中获取最大的星型子图
StarGraph& findbiggestStar(std::vector<StarGraph>& stars);

// 星型子图平衡处理
std::vector<StarGraph> balanceStars(std::vector<StarGraph> stars);

// 星型子图平衡处理，os存储打印信息
std::vector<StarGraph> balanceStars(std::vector<StarGraph> stars, std::ostream &os);

// 计算给定分解中有多少个size为m的星型子图
int countStarGraphsOfSize(const std::vector<StarGraph>& stars, int size);

/**
 * 计算自然连接查询的 MF 上界，支持相同连接键的多表连接
 * @param MF 每个关系表的属性值最大频率
 * @param N 每个关系表的大小
 * @return 连接结果的上界（整数）
 */
uint64_t computeMFBound(std::vector<uint64_t> MF, std::vector<uint64_t> N);

/**
 * 当连接键不唯一，且只能获取单列的最大频率时，计算自然连接查询的 MF 上界
 * @param MFs 每个关系表中作为连接键的列的最大频率集合，MFs[i]表示第i个连接键在各表中对应的MF值
 * @param N 每个关系表的大小
 * @return 连接结果的上界（整数）
 */
uint64_t computeMFBound(std::vector<std::vector<uint64_t>> MFs, std::vector<uint64_t> N);

uint64_t stepwiseMFBound(std::vector<uint64_t> MF, std::vector<uint64_t> N);

/**
 * 计算自然连接查询的 AGM 上界
 * @param V 所有属性的集合
 * @param E 超图的边集合，每个边对应一个关系表的属性集
 * @param N 每个关系表的大小
 * @return 连接结果的上界（整数）
 */
uint64_t computeAGMBound(std::set<std::string> V, std::vector<std::set<std::string>> E, std::vector<uint64_t> N);

/**
 * 计算自然连接查询的 AGM 上界
 * @param t1_headers 待连接表1的属性集
 * @param t2_headers 待连接表2的属性集
 * @param t1_size 待连接表1的大小
 * @param t2_size 待连接表1的大小
 * @return 连接结果的上界（整数）
 */
uint64_t computeAGMBound(std::vector<std::string> t1_headers, std::vector<std::string> t2_headers, uint64_t t1_size, uint64_t t2_size);

/// @brief 计算查询图的 AGM 上界
/// @param edges 查询图的边表
/// @param size 初始边匹配的size
/// @return 查询图的 AGM 上界
uint64_t computeAGMBound(const std::vector<std::vector<int>>& edges, uint64_t size);

/**
 * 计算星型拓扑结构的AGM上界，根据star_size可以自动构建超图结构，每一个边匹配的大小为相同的size
 * @param star_size 星型结构的size（边数）
 * @param size 星型结构中每一条边匹配的大小，要求是相同的
 * @return 连接结果的上界（整数）
 */
uint64_t computeStarAGMBound(u_int star_size, uint64_t size);


// 分步计算函数：按顺序两两连接，每次AGM使用当前待连接的两张表计算，返回最终上界
uint64_t stepwiseAGMBound(std::set<std::string> V, std::vector<std::set<std::string>> E, std::vector<uint64_t> N);

/// @brief 计算查询图的混合上界
/// @param edges 查询图的边表
/// @param N 边匹配的size
/// @param mf 边匹配各属性的最大频率
/// @return 查询的最紧混合上界
uint64_t mixedUpperBound(std::vector<std::vector<int>> edges, uint64_t N, uint64_t mf, std::vector<std::vector<int>> symmetries = {});

// 根据vec计算使其变成升序顺序的perm，vec = {v1, v2, ..., vm}, perm = {p1, p2, ..., pm}，vec_sorted = {v_{p1}, v_{p2}, ..., v_{pm}}
std::vector<int> getSortingPermutation(const std::vector<int>& vec);

// 根据排列 perm 对向量 vec 进行置换
template <typename T>
std::vector<T> applyPermutation(const std::vector<int>& perm, const std::vector<T>& vec) {
    // 检查输入合法性
    if (vec.size() != perm.size()) {
        throw std::invalid_argument("Invalid Argument Error in applyPermutation():Vector and permutation sizes must match.");
    }

    std::vector<T> result;
    result.reserve(perm.size());

    for (int idx : perm) {
        if (idx < 0 || idx >= static_cast<int>(vec.size())) { // 检查 0-based 索引合法性
            throw std::out_of_range("Out-of-Range Error in applyPermutation(): Invalid index in permutation.");
        }
        result.push_back(vec[idx]); // 直接使用 0-based 索引
    }

    return result;
}

}
