#pragma once
#include <iostream>
#include <chrono>
#include <stdexcept>
#include <functional>
#include <utility>
#include <sstream>
#include <fstream>
#include <unistd.h>

class Test {
private:
    std::function<void()> test_func_;                       // 绑定的测试函数
    std::chrono::steady_clock::time_point start_, end_;     // 时间点
    bool success_ = true;                                   // 测试是否成功标志
    std::string exception_;                                 // 异常信息
    std::size_t mem_usage_start_ = 0;                       // 测试前内存使用情况
    std::size_t mem_usage_end_ = 0;                         // 测试后内存使用情况

    // 获取当前内存使用情况（单位：字节）
    std::size_t getCurrentMemoryUsage() const {
        std::size_t rss = 0;
        std::ifstream stat_stream("/proc/self/status", std::ios_base::in);
        std::string line;
        while (std::getline(stat_stream, line)) {
            if (line.compare(0, 6, "VmRSS:") == 0) {
                std::istringstream iss(line);
                std::string key;
                std::size_t value;
                std::string unit;
                if (iss >> key >> value >> unit) {
                    // Linux下 VmRSS 单位通常为 kB，这里转换为字节
                    if (unit == "kB") {
                        rss = value * 1024;
                    }
                }
                break;
            }
        }
        return rss;
    }

public:
    // 构造函数：绑定测试函数及其参数
    template<typename Func, typename... Args>
    Test(Func&& func, Args&&... args) {
        // 利用 lambda 表达式捕获函数及其参数
        test_func_ = [func = std::forward<Func>(func), ...args = std::forward<Args>(args)]() mutable {
            std::invoke(func, args...);
        };
    }

    // 执行测试函数并测量时间
    void run() {
        success_ = true;
        exception_.clear();

        // 记录测试前的内存使用情况
        mem_usage_start_ = getCurrentMemoryUsage();
        // 记录测试前的时间
        start_ = std::chrono::steady_clock::now();

        try {
            test_func_();   // 执行测试函数
        } catch (const std::exception& e) {
            success_ = false;
            exception_ = e.what();
        } catch (...) {
            success_ = false;
            exception_ = "Unknown exception";
        }

        // 记录测试后的时间
        end_ = std::chrono::steady_clock::now();

        // 记录测试后的内存使用情况
        mem_usage_end_ = getCurrentMemoryUsage();
    }

    // 获取执行时间（默认单位为毫秒）
    template<typename Duration = std::chrono::milliseconds>
    long long getDuration() const {
        return std::chrono::duration_cast<Duration>(end_ - start_).count();
    }

    // 专门测算内存使用变化的函数，返回 MB 为单位的内存变化（执行后内存减去执行前内存）
    double getMemoryChange() const {
        std::ptrdiff_t diff = static_cast<std::ptrdiff_t>(mem_usage_end_) - static_cast<std::ptrdiff_t>(mem_usage_start_);
        return static_cast<double>(diff) / (1024 * 1024);
    }

    // 输出测试指标
    void printMetrics() const {
        if (!success_) {
            std::cout << "Test fail with exception: " << exception_ << std::endl;
        } else {
            std::cout << "Test success!" << std::endl;
        }
        
        std::cout << "Time cost: " << getDuration() << " ms" << std::endl;
        std::cout << "Memory cost: " << getMemoryChange() << " MB" << std::endl;
    }
};

// ------------------------ System Test ------------------------

void limit_test();

// ------------------------ Funtional Test ------------------------

void basic_test();

void table_test();

void graph_submatch_test();

void join_plan_test();

void table_index_test();

void table_bucketjoin_test();

void get_symmetries_test();

void compute_AGMBound_test();

void checkMatch_protocol_test();

void star_decomposition_test();

void mix_upper_bound_test();

void hash_join_test();

// ------------------------ DataSet Test ------------------------

void example_test(); 

void GRQC_test(); 

void Euroroads_test();

/// @brief 端到端全流程测试
/// @param G_file_name 数据图文件名
/// @param Q_file_name 查询图文件名
/// @param use_hierarchy 是否使用分层直方图构建索引
/// @param use_third_party 是否使用第三方功能
/// @param use_join_strategy 是否使用连接策略
/// @param add_noise 是否对统计结果加噪
/// @param e_idx 
/// @param e_mf 
void end_to_end_test(const std::string& G_file_name, const std::string& Q_file_name, bool use_hierarchy, bool use_third_party, bool use_join_strategy, bool add_noise, double e_idx, double e_mf);