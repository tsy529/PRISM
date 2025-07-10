#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <array>
#include <sstream>
#include <limits>

static const u_int NO_SHARING = 0;
static const u_int BINARY_SHARING = 1;
static const u_int ARITHMETIC_SHARING = 2;

static const int EMPTY = std::numeric_limits<int>::min();

static const u_int SHARE_PARTY_NUM = 3;

// 秘密共享的模数
static const int MOD = 1048576;

// 秘密x=x1+x2+x3, ShareTuple表示{x1,x2,x3}
class ShareTuple {
public:
    std::array<int, 3> data; 
    u_int type;

    // 最基础的构造函数，至少传入type
    ShareTuple(u_int type) : type (type) { data.fill(EMPTY); }

    // 复制构造函数
    ShareTuple(const ShareTuple&) = default;

    // 传入数组和type的构造函数
    ShareTuple(const std::array<int, 3> &data, u_int type) : data(data), type(type) {}

    // 传入整形自动生成三个满足要求的构造函数
    ShareTuple(int secret, u_int type);

    // 简化取值和比较操作
    int &operator[](u_int i) { return data[i]; }
    const int &operator[](u_int i) const { return data[i]; }
    bool operator==(const ShareTuple& other) const {
        return data == other.data && type == other.type;
    }

    // 字符串和SharingValue对象之间的转换
    void fromString(const std::string& str);
    void fromDataString(const std::string& str);
    std::string toString();
    std::string toDataString();

    // 检查data中每一位都被赋值过
    bool isFull() { return data[0] != EMPTY && data[1] != EMPTY && data[2] != EMPTY; }

    // 计算对象代表的秘密值
    int compute();
};

// S_i持有的SharePair为{x_i, x_{i+1}}
class SharePair {
public:
    std::array<int, 2> data;
    u_int type;
    
    // 默认构造函数，编译器自动生成
    SharePair() = default;

    // 默认拷贝构造函数，编译器自动生成
    SharePair(const SharePair&) = default;

    // 一一赋值构造函数
    SharePair(const std::array<int, 2>& d, u_int type) 
        : data(d), type(type) {};

    // 解析string构造函数
    SharePair(const std::string& str) { fromString(str); }
    
    SharePair& operator=(const SharePair& copy);
    SharePair operator+(const SharePair& rhs) const;
    SharePair operator-(const SharePair& rhs) const;
    SharePair operator^(const SharePair& rhs) const;

    int& operator[](u_int i) { return data[i]; }
    const int& operator[](u_int i) const { return data[i]; }
    bool operator==(const SharePair& other) const {
        return data == other.data;
    }

    void fromString(const std::string& str);
    void fromDataString(const std::string& str);
    std::string toString() const;
    std::string toDataString() const;
};

std::vector<SharePair> String2SPs(const std::string& str);
std::string SPs2String(const std::vector<SharePair>& sharePairs);