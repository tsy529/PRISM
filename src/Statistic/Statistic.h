#pragma once

#include <iostream>
#include <vector>
#include <queue>
#include <unordered_map>
#include <string>
#include <algorithm>
#include <cmath>
#include <sstream>

#include "../DP/Special.h"

using Range = std::pair<int, int>;      // 表示[low_value, high_value]这个区间

class Histogram {
private:
    std::vector<std::string> attrs;                 // 直方图涉及的属性
    std::vector<Range> ranges;                      // 各属性的值区间，ranges[i]=(low, high)表示attrs[i]的区间为[low, high]     
    std::unordered_map<std::string, int> counts;    // key是多个属性值组成的vector，value是该组合出现的次数

    // 检查值是否在对应范围
    bool isInRange(int value, const Range& range);

    int original_count;

public:
    // 构造函数
    Histogram(std::vector<std::string> &attrs, std::vector<std::vector<int>>& records, std::vector<Range>& ranges);
    Histogram(const std::vector<std::string>& attrs, const std::vector<Range>& ranges, const std::unordered_map<std::string, int>& counts);

    // Get
    int getCount(const std::vector<int>& values);
    std::vector<std::string> getAttrs() const { return attrs; }
    std::vector<Range> getRanges() const { return ranges; }
    const std::unordered_map<std::string, int>& getCounts() const { return counts; }
    int getMaxFrequency() const;
    int getTotalCount() const;
    int getOriginalCount() { return original_count; }

    // Set
    void setOriginalCount(int count) { this->original_count = count; }

    // 添加差分隐私噪声: type 0 随机噪声，-1 负向噪声， 1 正向噪声
    Histogram addDPNoise(double epsilon, int type);

    // 降维
    Histogram reduction(const std::vector<std::string>& sub_attrs);

    // 打印直方图
    std::string toString() const;
};

class HierarchicalHistogram {
private:
    struct Node {
        Range range;
        int count;
        Node* left;
        Node* right;

        Node (int start, int end) : range(start, end), count(0), left(nullptr), right(nullptr) {}
    };

    Node* root;
    Range original_range;
    int original_count;

    // 递归构造完全二叉树
    Node* buildTree(int start, int end, int depth);

    // 获取树的高度
    int getTreeHeight(Node* node) const;
    
    // 递归深拷贝树
    Node* cloneTree(Node* originalNode);

    // 递归计算聚合值
    int computeCounts(Node* node);

    // 递归查找叶子结点
    Node* findLeaf(Node* node, int value);

    // 递归范围查询
    int rangeQueryHelper(Node* node, int queryStart, int queryEnd, std::vector<Node*>& selectedNodes);

    // 递归地为树中每个节点添加噪声，并返回新的节点, type 0 随机噪声，-1 负向噪声， 1 正向噪声
    Node* cloneNodeWithNoise(Node* originalNode, double epsilon_per_level, int type);

    // 打印结点
    std::string formatNode(Node* node) const;
    std::string nodeToString(Node* node, bool isLast, std::string prefix) const;

public:

    HierarchicalHistogram(const std::unordered_map<std::string, int>& counts, const Range& range);
    // 拷贝构造函数（用于创建新的直方图对象）
    HierarchicalHistogram(const Node* root, const Range& range, const int total_count);

    // 获取原始数值范围
    Range getOriginalRange() const { return original_range; }

    // 获取原始数值范围
    int getOriginalCount() const { return original_count; }

    // 获取所有叶子结点中 count 的最大值
    int getMaxLeaf();

    // 范围查询函数
    std::string rangeQueryWithDetail(int queryStart, int queryEnd);

    // 添加差分隐私噪声: type 0 随机噪声，-1 负向噪声， 1 正向噪声
    HierarchicalHistogram addDPNoise(double epsilon, int type);

    int rangeQuery(int queryStart, int queryEnd);
    std::string toStringByTree() const;
    std::string toStringByLevel() const;
};

class Index {
private:
    // 属性attrs[i]的值范围为ranges[i]，使用widths[i]对该属性进行分割
    std::vector<std::string> attrs;         // 属性名称集合
    std::vector<Range> ranges;              // 各属性的值区间
    std::vector<int> widths;                // 各属性划分宽度

    // 区间sorted_intervals[i]的起止位置是starts[i]，ends[i]
    std::vector<int> starts;                // 区间起始位置
    std::vector<int> ends;                  // 区间结束位置
    std::vector<std::pair<std::string, int>> sorted_intervals;  // 存储排序后的区间编号以及对应的频次

    // 隐私参数
    double epsilon = 0.5;   // 默认0.5

public:
    // Index(const Histogram& histogram, const std::vector<int>& widths);

    // 构造函数， noise: true表示使用噪声
    Index(const Histogram& histogram, const std::vector<int>& widths, bool noise, double epsilon = 1.5);

    /// @brief 使用加噪后的h+和h-直接构造一维 DP Index
    /// @param h_p h+
    /// @param h_n h-
    Index(Histogram& h_p, Histogram& h_n);

    /// @brief 使用加噪后的hh+和hh-直接构造一维 DP Index
    /// @param attr
    /// @param hh_p hh+
    /// @param hh_n hh-
    Index(std::string attr, HierarchicalHistogram& hh_p, HierarchicalHistogram& hh_n);
    Index(std::vector<std::string> attrs, 
          std::vector<Range> ranges, 
          std::vector<int> widths, 
          std::vector<int> starts,
          std::vector<int> ends, 
          std::vector<std::pair<std::string, int>> sorted_intervals)
        : attrs(attrs), ranges(ranges), widths(widths), starts(starts), ends(ends), sorted_intervals(sorted_intervals) {}

    // 根据属性值，确定属于哪个区间，返回区间的起始位置
    Range findInterval(const std::vector<int>& values) const;

    // Get
    std::vector<std::string> getAttrs() const { return attrs; }
    std::vector<Range> getRanges() const { return ranges; }
    std::vector<Range> getRanges(int idx) const;
    std::vector<int> getWidths() const { return widths; }
    const std::vector<int>& getStarts() const { return starts; }
    const std::vector<int>& getEnds() const { return ends; }
    const std::vector<std::pair<std::string, int>>& getSortedIntervals() const { return sorted_intervals; }

    // Set
    void setAttrs(std::vector<std::string> attrs) { this->attrs = attrs; }

    // 打印索引
    std::string toString() const;
};

// Key（多维int值）的vec<int>和string形式的转换
std::string vec2Key(const std::vector<int> &values);
std::vector<int> key2Vec(const std::string &key);
