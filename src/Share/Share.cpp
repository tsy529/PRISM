#include "Share.h"

ShareTuple::ShareTuple(int value, u_int type) {
    this->type = type;

    if (type == ARITHMETIC_SHARING) {
        data[0] = std::rand() % MOD;
        data[1] = std::rand() % MOD;
        data[2] = (value - data[0] - data[1]) % MOD;
        data[2] = data[2] < 0 ? data[2] + MOD : data[2];
    } else {
        data[0] = std::rand() % MOD;
        data[1] = std::rand() % MOD;
        data[2] = value ^ data[0] ^ data[1];
    }
}

void ShareTuple::fromString(const std::string& str) {
    // "type s0,s1,s2"
    std::istringstream iss(str);
    std::string token;

    // 先读取类型
    iss >> type;

    // 跳过空格
    iss.ignore();

    for (int i = 0; i < 3; i++) {
        if (!std::getline(iss, token, ',')) {
            throw std::invalid_argument("Invalid argument in fromString(): Invalid ShareTuple format");
        }
        data[i] = std::stoi(token);
    }
}

void ShareTuple::fromDataString(const std::string& str) {
    std::istringstream iss(str);
    std::string token;

    for (int i = 0; i < 3; i++) {
        if (!std::getline(iss, token, ',')) {
            throw std::invalid_argument("Invalid argument in fromString(): Invalid ShareTuple format");
        }
        data[i] = std::stoi(token);
    }
}

std::string ShareTuple::toString() {
    return std::to_string(type) + " " + std::to_string(data[0]) + "," + std::to_string(data[1]) + "," + std::to_string(data[2]);
}

std::string ShareTuple::toDataString() {
    return std::to_string(data[0]) + "," + std::to_string(data[1]) + "," + std::to_string(data[2]);
}

int ShareTuple::compute() {
    if (type == ARITHMETIC_SHARING) {
        int res = (data[0] + data[1] + data[2]) % MOD;
        return res < 0 ? res + MOD : res; //  确保落在[0,MOD)
    } else {
        return data[0] ^ data[1] ^ data[2];
    }
}

SharePair &SharePair::operator=(const SharePair &copy) {
    data[0] = copy.data[0];
    data[1] = copy.data[1];
    type = copy.type;
    return *this;
}

SharePair SharePair::operator+(const SharePair &rhs) const {
    if (this->type != ARITHMETIC_SHARING || rhs.type != ARITHMETIC_SHARING) {
        std::cerr << "Error operand types" << std::endl;
    }

    SharePair result;
    result.data[0] = (data[0] + rhs.data[0]) % MOD;
    result.data[1] = (data[1] + rhs.data[1]) % MOD;
    result.type = this->type;
    return result;
}

SharePair SharePair::operator-(const SharePair &rhs) const {
    if (this->type != ARITHMETIC_SHARING || rhs.type != ARITHMETIC_SHARING) {
        std::cerr << "Error operand types" << std::endl;
    }

    SharePair result;
    result.data[0] = (data[0] - rhs.data[0]) % MOD;
    result.data[1] = (data[1] - rhs.data[1]) % MOD;
    result.type = this->type;
    return result;
}

SharePair SharePair::operator^(const SharePair &rhs) const {
    if (this->type != BINARY_SHARING || rhs.type != BINARY_SHARING) {
        std::cerr << "Error operand types" << std::endl;
    }

    SharePair result;
    result.data[0] = data[0] ^ rhs.data[0];
    result.data[1] = data[1] ^ rhs.data[1];
    result.type = this->type;
    return result;
}

void SharePair::fromString(const std::string& str) {
    // "type s_i,s_{i+1}"
    std::istringstream iss(str);
    std::string token;

    // 读取type
    iss >> type;

    // 跳过空格
    iss.ignore();

    for (int i = 0; i < 2; i++) {
        if (!std::getline(iss, token, (i == 0 ? ',' : '\n'))) {
            throw std::invalid_argument("Invalid argument in fromString(): Invalid SharePair format");
        }
        data[i] = std::stoi(token);
    }
}

void SharePair::fromDataString(const std::string& str) {
    // "s_i,s_{i+1}"
    std::istringstream iss(str);
    std::string token;

    for (int i = 0; i < 2; i++) {
        if (!std::getline(iss, token, (i == 0 ? ',' : '\n'))) {
            throw std::invalid_argument("Invalid argument in fromString(): Invalid SharePair format");
        }
        data[i] = std::stoi(token);
    }
}

std::string SharePair::toString() const {
    return std::to_string(type) + " " + std::to_string(data[0]) + "," + std::to_string(data[1]);
}

std::string SharePair::toDataString() const {
    return std::to_string(data[0]) + "," + std::to_string(data[1]);
}


std::vector<SharePair> String2SPs(const std::string& str) {
    std::vector<SharePair> result;
    std::istringstream iss(str);
    std::vector<std::string> tokens;
    std::string token;

    // 分割所有token
    while (iss >> token) {
        tokens.push_back(token);
    }

    // 每两个token组成一个SharePair
    for (size_t i = 0; i + 1 < tokens.size(); i += 2) {
        std::string pairStr = tokens[i] + " " + tokens[i + 1];
        result.emplace_back(SharePair(pairStr));
    }

    return result;
}

std::string SPs2String(const std::vector<SharePair>& sharePairs) {
    std::ostringstream oss;
    for (size_t i = 0; i < sharePairs.size(); ++i) {
        if (i != 0) {
            oss << " "; // 元素间用空格分隔
        }
        oss << sharePairs[i].toString();
    }
    return oss.str();
}