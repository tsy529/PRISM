#pragma once

#include <vector>
#include <map>
#include <iostream>
#include <string>
#include <sstream>

class Message {
public:
    u_int from;
    u_int to;
    u_int command;

    // 构造函数
    Message() = default;
    Message(u_int from, u_int to, u_int command) : from(from), to(to), command(command) {}
    Message(const std::string& data) {
        std::istringstream iss(data);
        std::string line;
        
        while(std::getline(iss, line)) {
            auto pos = line.find(':');
            auto key = line.substr(0, pos);
            auto value = line.substr(pos + 1);

            if (key == "from") from = std::stoul(value);
            else if (key == "to") to = std::stoul(value);
            else if (key == "command") command = std::stoul(value);
        }
    }
    
    // 虚函数
    virtual std::string toString() const {
        std::ostringstream oss;
        oss << "from:" << std::to_string(from) << "\n"
            << "to:" << std::to_string(to) << "\n"
            << "command:" << std::to_string(command) << "\n";
        return oss.str();
    }
};

// 基础指令信息，不附带其他信息的指令消息：RESPONSE_COMMAND, LOADING_COMMAND
class BscMessage : public Message {
public:
    BscMessage(u_int from, u_int to, u_int command) : Message(from, to, command) {}

    BscMessage(const std::string& data) {
        std::istringstream iss(data);
        std::string line;
        
        while(std::getline(iss, line)) {
            auto pos = line.find(':');
            auto key = line.substr(0, pos);
            auto value = line.substr(pos + 1);

            if (key == "from") from = std::stoul(value);
            else if (key == "to") to = std::stoul(value);
            else if (key == "command") command = std::stoul(value);
        }
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << "from:" << std::to_string(from) << "\n"
            << "to:" << std::to_string(to) << "\n"
            << "command:" << std::to_string(command) << "\n";
        return oss.str();
    }
};

// 操作指令信息，携带输入变量和输出变量的信息
class OprMessage : public Message {
public:
    std::vector<std::string> input_names;
    std::string output_name;

    OprMessage(u_int from, u_int to, u_int command, std::vector<std::string>& input_names, const std::string& output_name)
        : Message(from, to, command), input_names(input_names), output_name(output_name) {}

    OprMessage(const std::string& data) {
        std::istringstream iss(data);
        std::string line;
        
        while(std::getline(iss, line)) {
            auto pos = line.find(':');
            auto key = line.substr(0, pos);
            auto value = line.substr(pos + 1);

            if (key == "from") from = std::stoul(value);
            else if (key == "to") to = std::stoul(value);
            else if (key == "command") command = std::stoul(value);
            else if (key == "input_names") {
                std::string names_str = value;
                std::istringstream names_stream(names_str);
                std::string name;
                while (names_stream >> name) {
                    input_names.push_back(name);
                }
            } 
            else if (key == "output_name") output_name = value;
        }
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << "from:" << std::to_string(from) << "\n"
            << "to:" << std::to_string(to) << "\n"
            << "command:" << std::to_string(command) << "\n";
        oss << "input_names:";
        for (int i = 0; i < input_names.size(); i++) {
            oss << input_names[i];
            if (i != input_names.size() - 1) {
                oss << " ";
            } else {
                oss << "\n";
            }
        } 
        oss << "output_name:" << output_name << "\n";
        return oss.str();
    }
};

// 传值指令信息：携带被传递的值的信息
class VarMessage : public Message {
public:
    std::string var_name;
    std::string var_str;
    std::string result_name;

    VarMessage(u_int from, u_int to, u_int command, const std::string& var_name, const std::string& var_str) 
        : Message(from, to, command), var_name(var_name), var_str(var_str), result_name(var_name) {}

    VarMessage(u_int from, u_int to, u_int command, const std::string& var_name, const std::string& var_str, const std::string& result_name) 
        : Message(from, to, command), var_name(var_name), var_str(var_str), result_name(result_name) {}

    VarMessage(const std::string& data) {
        std::istringstream iss(data);
        std::string line;
        
        while(std::getline(iss, line)) {
            auto pos = line.find(':');
            auto key = line.substr(0, pos);
            auto value = line.substr(pos + 1);

            if (key == "from") from = std::stoul(value);
            else if (key == "to") to = std::stoul(value);
            else if (key == "command") command = std::stoul(value);
            else if (key == "var_name") var_name = value;
            else if (key == "var_str") var_str = value;
            else if (key == "result_name") result_name = value;
            else var_str += "\n" + line;
        }

        if (result_name.empty()) {
            result_name = var_name;
        }
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << "from:" << std::to_string(from) << "\n"
            << "to:" << std::to_string(to) << "\n"
            << "command:" << std::to_string(command) << "\n"
            << "var_name:" << var_name << "\n"
            << "var_str:" << var_str << "\n"
            << "result_name:" << result_name << "\n";

        return oss.str();
    }
};

// 多值传递指令信息：携带被传递的值的信息
class VarsMessage : public Message {
public:
    std::vector<std::pair<std::string, std::string>> vars;
    std::string result_name;

    // 参数化构造函数
    VarsMessage(u_int from, u_int to, u_int command, const std::vector<std::pair<std::string, std::string>> &vars)
        : Message(from, to, command), vars(vars) {}

    VarsMessage(u_int from, u_int to, u_int command, const std::vector<std::pair<std::string, std::string>> &vars, const std::string& result_name)
        : Message(from, to, command), vars(vars), result_name(result_name) {}

    // 反序列化构造函数
    VarsMessage(const std::string &data) {
        std::istringstream iss(data);
        std::string line;
        size_t var_count = 0;
        std::map<size_t, std::pair<std::string, std::string>> var_map;

        while (std::getline(iss, line)) {
            auto pos = line.find(':');
            if (pos == std::string::npos)
                continue;
            std::string key = line.substr(0, pos);
            std::string value = line.substr(pos + 1);

            if (key == "from") {
                from = std::stoul(value);
            } else if (key == "to") {
                to = std::stoul(value);
            } else if (key == "command") {
                command = std::stoul(value);
            } else if (key == "var_count") {
                var_count = std::stoul(value);
            } else if (key.substr(0, 3) == "var") {
                size_t num_end = key.find('_', 3);
                if (num_end != std::string::npos) {
                    std::string num_str = key.substr(3, num_end - 3);
                    try {
                        size_t index = std::stoul(num_str);
                        std::string suffix = key.substr(num_end + 1);
                        if (suffix == "name") {
                            var_map[index].first = value;
                        } else if (suffix == "str") {
                            var_map[index].second = value;
                        }
                    } catch (...) {
                        // 忽略无效的索引格式
                    }
                }
            } else if (key == "result_name") {
                result_name = value;
            }
        }

        if (var_count == 0 && !var_map.empty()) {
            var_count = var_map.rbegin()->first + 1;
        }

        vars.reserve(var_count);
        for (size_t i = 0; i < var_count; ++i) {
            auto it = var_map.find(i);
            if (it != var_map.end() && !it->second.first.empty() && !it->second.second.empty()) {
                vars.emplace_back(it->second.first, it->second.second);
            } else {
                vars.emplace_back("", "");
            }
        }
    }

    // 序列化方法
    /*
        from:1
        to:2
        command:3
        var_count:2
        var0_name:temperature
        var0_str:23.5
        var1_name:humidity
        var1_str:60%\
        result_name:result
    */
    std::string toString() const override {
        std::ostringstream oss;
        oss << Message::toString();
        oss << "var_count:" << vars.size() << "\n";
        for (size_t i = 0; i < vars.size(); ++i) {
            oss << "var" << i << "_name:" << vars[i].first << "\n"
                << "var" << i << "_str:" << vars[i].second << "\n";
        }
        oss << "result_name:" << result_name << "\n";
        return oss.str();
    }
};