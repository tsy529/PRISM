#pragma once

#include <vector>
#include <unordered_map>
#include <set>
#include <cmath>
#include <string>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <any>
#include <numeric>
#include <random>
#include <glpk.h>
#include "../Share/Share.h"
#include "../Statistic/Statistic.h"
#include "../Function/Function.h"

class Share;
class ShareTuple;

using TableMode = u_int;

static const uint64_t CARTESIAN_BOUND = UINT64_MAX;
static const uint64_t MF_BOUND = UINT64_MAX - 1;
static const uint64_t AGM_BOUND = UINT64_MAX - 2;
extern uint64_t AGM_BOUND_;
extern uint64_t MIX_BOUND_;
static const uint64_t MIX_AGM_MF_BOUND = UINT64_MAX - 3;
static const uint64_t MIX_BOUND = UINT64_MAX - 4;

class Table {
public:
    std::vector<std::string> headers;
    std::vector<u_int> share_types;
    u_int mode;  // int, SharePair or ShareTuple
    std::vector<std::vector<std::any>> data;
    std::vector<uint64_t> max_freqs;                // 为每一列设置属性值的最大频率 
    std::vector<std::any> is_null;                  // 是否为NULL的标识符数组，非INT_MODE情况下，共享类型固定位布尔类型

    static const TableMode INT_MODE = 0;
    static const TableMode SHARE_TUPLE_MODE = 1;
    static const TableMode SHARE_PAIR_MODE = 2;

    double epsilon = 1.5;                          // 隐私参数

    // 构造函数
    // 需要确定表的mode
    Table(TableMode mode) : mode(mode) {};
    // 拷贝构造
    Table(const Table&) = default;

    // 辅助函数，按空格分割字符串
    std::vector<std::string> split(const std::string& str);
    // 辅助函数，验证某条记录的列数是否与表头匹配
    void validateData(const std::vector<std::any>& row);
    // 辅助函数，特定share类型的对象与string之间的相互转换
    std::any convertStringToData(const std::string& str, u_int share_type);
    std::string convertDataToString(const std::any& value) const;

    // 读取文件中的表
    void readFromFile(const std::string& filename);
    // 读取字符串表示的表
    void readFromString(const std::string& str);
    // 将表转为字符串
    std::string toString() const;
    // 插入新的一列，自动填充0值
    void addColumn(const std::string& column_name, u_int share_type);
    // 插入新的一列，且提供该列数据
    void addColumn(const std::string& column_name, u_int share_type, const std::vector<std::any>& column_data);
    // 取出指定单列，放在一个数组中输出
    std::vector<std::any> getColumn(const std::string& col_name);
    // 获取制定单列列号
    int getColumnIdx(const std::string& col_name) const;
    // 取出指定多列（是不是也该换成用Table输出结果？）
    std::vector<std::vector<std::any>> select(std::vector<std::string> col_names) const;
    // 以Int类型取出多列
    std::vector<std::vector<int>> selectAsInt(std::vector<std::string> col_names) const;
    // 提取表中特定行范围
    Table slice(int start, int end) const;
    // 拼接表
    void concate(const Table& table);
    // 根据属性数组排序，数组表示排序时属性的优先级，只能对INT_MODE进行排序
    void sortBy(std::vector<std::string> attrs);
    // 设置表头
    void setHeaders(const std::vector<std::string>& new_headers);
    // 获取指定列的最大最小值
    Range getColumnValueRange(int col_idx) const;
    // 获取所有列的最大最小值
    std::vector<Range> getAllColumnValueRange() const;
    // 填充dummy行
    void padding(const uint64_t& padding_to = 0);
    void padding(const uint64_t& padding_to, std::vector<Range> value_ranges);
    // 清除dummy行
    void clearDummy();
    // 格式化数据，使headers升序排列
    void normalize();
    // 计算max_freqs
    void calcualteMaxFreqs();
    // 为max_freqs加噪
    void addNoiseToMFs();

};

// 打印函数
void log(const std::string& log);
void log_process(size_t current, size_t total);

// 基本的Join操作
Table Join(const Table& table1, const Table& table2);
Table Join(const Table& table1, const Table& table2, const uint64_t& padding_to);

Table Join_Hash(const Table& table1, const Table& table2);

// 按桶连接。要求两表必须有索引，且是根据索引排序的。
std::pair<Table, Index> BucketJoin(const Table& table1, const Table& table2, const Index& table1_idx, const Index& table2_idx, const uint64_t& padding_to = 0);

std::vector<std::string> getJoinKey(const Table& table1, const Table& table2);