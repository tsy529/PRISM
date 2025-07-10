#include "Table.h"

#include "Table.h"

const std::string print_color = "\033[94m";
const std::string default_color = "\033[0m";

bool enable_log = true;
std::mutex table_print_mutex;

std::vector<std::string> Table::split(const std::string& str) {
    std::vector<std::string> tokens;
    std::istringstream iss(str);
    std::string token;
    while (iss >> token) {
        tokens.push_back(token);
    }
    return tokens;
}

void Table::validateData(const std::vector<std::any>& row) {
    if (row.size() != headers.size()) {
        throw std::invalid_argument("Invalid argument in validateData(): Data row size does not match headers size");
    }
}

std::any Table::convertStringToData(const std::string& str, u_int share_type) {
    if (mode == INT_MODE) {
        return std::any(std::stoi(str));
    } else if (mode == SHARE_TUPLE_MODE) {
        ShareTuple sv(share_type);
        sv.fromDataString(str);
        return std::any(sv);
    } else if (mode == SHARE_PAIR_MODE) {
        SharePair s;
        s.type = share_type;
        s.fromDataString(str);
        return std::any(s);
    }
    throw std::invalid_argument("Invalid argument in convertStringToData(): Unknown data type: " + mode);
}

std::string Table::convertDataToString(const std::any& value) const {
    if (mode == INT_MODE) {
        return std::to_string(std::any_cast<int>(value));
    } else if (mode == SHARE_TUPLE_MODE) {
        return std::any_cast<ShareTuple>(value).toDataString();
    } else if (mode == SHARE_PAIR_MODE) {
        return std::any_cast<SharePair>(value).toDataString();
    }
    throw std::invalid_argument("Invalid argument in convertDataToString(): Unknown data type: " + mode);
}

void Table::readFromFile(const std::string& filename) {
    std::ifstream file(filename);
    if (!file) {
        throw std::runtime_error("Runtime error in readFromFile(): Cannot open file: " + filename);
    }
    
    headers.clear();
    data.clear();
    is_null.clear();

    int is_null_idx;
    
    std::string line;
    if (std::getline(file, line)) {     // 第一行是表头
        headers = split(line);
        auto it = find(headers.begin(), headers.end(), "isNull");
        is_null_idx = it != headers.end() ? it - headers.begin() : -1;
    }

    if (std::getline(file, line)) {     // 第二行是各列的共享类型
        auto types = split(line);
        if (types.size() != headers.size()) {
            throw std::invalid_argument("Invalid argument in readFromFile(): Type row size does not match headers size");
        }

        if (this->mode == SHARE_PAIR_MODE || this->mode == SHARE_TUPLE_MODE) {
            for (const auto& type : types) {
                if (type == "a") {
                    share_types.push_back(ARITHMETIC_SHARING);
                } else if (type == "b") {
                    share_types.push_back(BINARY_SHARING);
                } else {
                    throw std::invalid_argument("Invalid argument in readFromFile(): Type row does not match table mode");
                }
            } 
        } else {
            for (const auto& type : types) {
                if (type == "p") {
                    share_types.push_back(NO_SHARING);
                } else {
                    throw std::invalid_argument("Invalid argument in readFromFile(): Type row does not match table mode");
                }
            }
        }
    }

    // 读取文件中的表不需要手动输入max_freqs信息
    
    while (std::getline(file, line)) {
        std::vector<std::any> row;
        std::istringstream iss(line);
        std::string value;
        int i = 0;  // 记录是第几列的数据，用于确定此列对应的共享类型
        while (iss >> value) {
            if (i == is_null_idx) {
                is_null.push_back(convertStringToData(value, share_types[i]));
            } else {
                row.push_back(convertStringToData(value, share_types[i]));
            }
            i++;
        }
        data.push_back(row);
    }

    if (is_null_idx != -1) {
        headers.erase(headers.begin() + is_null_idx);
        share_types.erase(share_types.begin() + is_null_idx);
    } else {
        // 为默认INT_MODE的输入文件添加isNull
        if (mode == INT_MODE) {
            is_null.assign(data.size(), 0);
        }
    }

    // 对于 INT_MODE 的表，读取文件时会主动计算 max_freq 值
    this->calcualteMaxFreqs();
}

void Table::readFromString(const std::string& str) {
    std::istringstream iss(str);
    
    headers.clear();
    data.clear();
    is_null.clear();
    
    int is_null_idx;
    
    std::string line;
    if (std::getline(iss, line)) {
        headers = split(line);
        auto it = find(headers.begin(), headers.end(), "isNull");
        is_null_idx = it != headers.end() ? it - headers.begin() : -1;
    }

    if (std::getline(iss, line)) {     // 第二行是各列的共享类型
        auto types = split(line);
        if (types.size() != headers.size()) {
            throw std::invalid_argument("Invalid argument in readFromString(): Type row size does not match headers size");
        }

        for (const auto& type : types) {
            if (this->mode == SHARE_PAIR_MODE || this->mode == SHARE_TUPLE_MODE) {
                if (type == "a") {
                    share_types.push_back(ARITHMETIC_SHARING);
                } else if (type == "b") {
                    share_types.push_back(BINARY_SHARING);
                } else {
                    throw std::invalid_argument("Invalid argument in readFromString(): Type row does not match table mode");
                }
            } else {
                if (type == "p") {
                    share_types.push_back(NO_SHARING);
                } else {
                    throw std::invalid_argument("Invalid argument in readFromString(): Type row does not match table mode");
                }
            }
        }
    }

    // 如果是通过字符串读取表，则第三行已经被设定为最大频率信息，读取即可
    if (std::getline(iss, line)) {     // 第三行是最大频率
        auto mfs_str = split(line);
        for (auto const& mf_str : mfs_str) {
            max_freqs.push_back(std::stoull(mf_str));
        }
    }
    
    while (std::getline(iss, line)) {
        std::vector<std::any> row;
        std::istringstream lineStream(line);
        std::string value;
        int i = 0;  // 记录是第几列的数据，用于确定此列对应的共享类型
        while (lineStream >> value) {
            if (i == is_null_idx) {
                is_null.push_back(convertStringToData(value, share_types[i]));
            } else {
                row.push_back(convertStringToData(value, share_types[i]));
            }
            i++;
        }
        data.push_back(row);
    }

    if (is_null_idx != -1) {
        headers.erase(headers.begin() + is_null_idx);
        share_types.erase(share_types.begin() + is_null_idx);
    }

}

std::string Table::toString() const {
    std::ostringstream oss;
    
    // 转换表头
    for (size_t i = 0; i < headers.size(); ++i) {
        oss << headers[i];
        if (i < headers.size() - 1) {
            oss << " ";
        } else {
            if (!is_null.empty()) {
                oss << " isNull";
            }
            oss << "\n";
        }
    }
    
    // 转换共享类型
    for (size_t i = 0; i < share_types.size(); ++i) {
        if (share_types[i] == NO_SHARING) {
            oss << "p";
        } else if (share_types[i] == BINARY_SHARING) {
            oss << "b";
        } else if (share_types[i] == ARITHMETIC_SHARING) {
            oss << "a";
        } 

        if (i < share_types.size() - 1) {
            oss << " ";
        } else {
            if (!is_null.empty()) {
                if (mode == INT_MODE) {
                    oss << " p";
                } else {
                    oss << " b";
                }
            }
        }
    }

    // 转换最大频率
    oss << "\n";
    for (const auto& max_freq : max_freqs) {
        oss << std::to_string(max_freq) << " ";
    }
    
    // 转换数据
    for (int i = 0; i < data.size(); i++) {
        oss << "\n";
        for (size_t j = 0; j < data[i].size(); ++j) {
            oss << convertDataToString(data[i][j]);
            if (j < data[i].size() - 1) {
                oss << " ";
            } else {
                if (!is_null.empty()) {
                    oss << " " + convertDataToString(is_null[i]);
                }
            }
        }
    }
    
    return oss.str();
}

// TODO 添加新增列的最大频率统计
void Table::addColumn(const std::string& column_name, u_int share_type) {       
    // 检查列名是否存在
    if (std::find(headers.begin(), headers.end(), column_name) != headers.end()) {
        throw std::invalid_argument("Bad argument in addColumn(): Column header already exists");
    }

    // 检查share_type的合法性
    if (mode == INT_MODE) {
        if (share_type != NO_SHARING) {
            throw std::invalid_argument("Bad argument in addColumn(): INT mode must use NO_SHARING type");
        }  
    } else {
        if (share_type != ARITHMETIC_SHARING && share_type != BINARY_SHARING) {
            throw std::invalid_argument("Bad argument in addColumn(): Non-INT mode must use ARITHMETIC_SHARING or BINARY_SHARING type");
        }
    }

    headers.push_back(column_name);
    share_types.push_back(share_type);
    
    for (u_int i = 0; i < data.size(); i++) {
        switch (mode) {
            case INT_MODE: {
                data[i].push_back(0);
                break;
            }
            case SHARE_TUPLE_MODE: {
                data[i].push_back(ShareTuple({0, 0, 0}, share_type));
                break;
            }
            case SHARE_PAIR_MODE: {
                data[i].push_back(SharePair({0, 0}, share_type));
                break;
            }
            default:
                throw std::runtime_error("Runtime error in addColumn(): Unsupported data mode");
        }
    }
}

// TODO 添加新增列的最大频率统计
void Table::addColumn(const std::string& column_name, u_int share_type, const std::vector<std::any>& column_data) {
    // 检查列名是否存在
    if (std::find(headers.begin(), headers.end(), column_name) != headers.end()) {
        throw std::invalid_argument("Bad argument in addColumn(): Column header already exists");
    }

    // 检查share_type的合法性
    if (mode == INT_MODE) {
        if (share_type != NO_SHARING) {
            throw std::invalid_argument("Bad argument in addColumn(): INT mode must use NO_SHARING type");
        }  
    } else {
        if (share_type != ARITHMETIC_SHARING && share_type != BINARY_SHARING) {
            throw std::invalid_argument("Bad argument in addColumn(): Non-INT mode must use ARITHMETIC_SHARING or BINARY_SHARING type");
        }
    }

    // 检查数据长度是否与现有行匹配
    if (!data.empty() && column_data.size() != data.size()) {
        throw std::invalid_argument("Bad argument in addColumn(): new column size doesn't match existing rows");
    }

    headers.push_back(column_name);
    share_types.push_back(share_type);

    // 如果是第一列，则直接添加数据
    if (data.empty()) {
        data.resize(column_data.size());
    }

    // 添加新列数据
    for (u_int i = 0; i < column_data.size(); i++) {
        data[i].push_back(column_data[i]);
    }
}

std::vector<std::any> Table::getColumn(const std::string& col_name) {
    // 查找列名对应的索引
    auto it = std::find(headers.begin(), headers.end(), col_name);
    if (it == headers.end()) {
        throw std::invalid_argument("Invalid argument in Table::getColumn(): Column name not found");
    }
    
    size_t col_idx = std::distance(headers.begin(), it);
    
    // 创建结果向量并预分配空间
    std::vector<std::any> result;
    result.reserve(data.size());
    
    // 提取指定列的所有数据
    for (const auto& row : data) {
        result.push_back(row[col_idx]);
    }
    
    return result;
}

int Table::getColumnIdx(const std::string& col_name) const {
    auto it = std::find(headers.begin(), headers.end(), col_name);
    if (it == headers.end())
        return -1;
    return std::distance(headers.begin(), it);
}

std::vector<std::vector<std::any>> Table::select(std::vector<std::string> col_names) const {
    std::vector<int> col_idxs;
    for (auto col_name : col_names) {
        int idx = getColumnIdx(col_name);
        if (idx == -1) {
            throw std::runtime_error("Runtime Error in Table::getColumns: no such col called " + col_name);
        }
        col_idxs.push_back(idx);
    }

    std::vector<std::vector<std::any>> columns;
    for (auto item : this->data) {
        std::vector<std::any> row;
        for (auto idx : col_idxs) {
            row.push_back(item[idx]);
        }
        columns.push_back(row);
    }

    return columns;
}

/*
std::vector<std::vector<std::any>> Table::getColumns(std::vector<std::string> col_names) {
    // 找到所需列的索引
    std::vector<size_t> col_indices;
    col_indices.reserve(col_names.size());
    
    for (const auto& name : col_names) {
        auto it = std::find(headers.begin(), headers.end(), name);
        if (it == headers.end()) {
            throw std::invalid_argument("Column name '" + name + "' not found");
        }
        col_indices.push_back(std::distance(headers.begin(), it));
    }
    
    // 创建结果矩阵并预分配空间
    std::vector<std::vector<std::any>> result(col_names.size());
    for (auto& col : result) {
        col.reserve(data.size());
    }
    
    // 按列填充数据
    for (size_t i = 0; i < data.size(); ++i) {
        for (size_t j = 0; j < col_indices.size(); ++j) {
            result[j].push_back(data[i][col_indices[j]]);
        }
    }
    
    return result;
}
*/

std::vector<std::vector<int>> Table::selectAsInt(std::vector<std::string> col_names) const {
    // 要求表本身必须是INT_MODE
    if (this->mode != INT_MODE) {
        throw std::runtime_error("Runtime Error in Table::selectAsInt(): Table has to be INT_MODE");
    }
    // 提取每一行并转为Int
    auto columns = this->select(col_names);
    std::vector<std::vector<int>> columns_int;
    for (auto& row : columns) {
        std::vector<int> row_int;
        for (auto& item : row) {
            row_int.push_back(std::any_cast<int>(item));
        }
        columns_int.push_back(row_int);
    }

    return columns_int;
}

Table Table::slice(int start, int end) const {
    Table t_slice(this->mode);
    t_slice.headers = this->headers;
    t_slice.share_types = this->share_types;
    t_slice.max_freqs = this->max_freqs;        // 添加的最大频率统计

    // 复制指定范围的数据和isnull标识符
    for (int i = start; i < end; i++) {
        t_slice.data.push_back(this->data[i]);
        t_slice.is_null.push_back(this->is_null[i]);
    }

    return t_slice;
}

void Table::concate(const Table& t) {       // 用在BucketJoin中，由于拆分为bucket的时候并没有拆分max_freqs，所以合并也不需要处理
    // 检查两个表的列数和类型是否匹配
    if (headers.size() != t.headers.size() || share_types != t.share_types) {
        throw std::invalid_argument("Invalid argument in Table::concate(): Tables have different structure");
    }
    
    // 检查列名是否匹配
    for (size_t i = 0; i < headers.size(); ++i) {
        if (headers[i] != t.headers[i]) {
            throw std::invalid_argument("Invalid argument in Table::concate(): Column names don't match");
        }
    }
    
    // 预先分配内存以提高性能
    data.reserve(data.size() + t.data.size());
    is_null.reserve(is_null.size() + t.is_null.size());
    
    // 将新表的数据追加到当前表
    data.insert(data.end(), t.data.begin(), t.data.end());
    is_null.insert(is_null.end(), t.is_null.begin(), t.is_null.end());
}

void Table::sortBy(std::vector<std::string> attrs) {
    // 要求表本身必须是INT_MODE
    if (this->mode != INT_MODE) {
        throw std::runtime_error("Runtime Error in Table::selectAsInt(): Table has to be INT_MODE");
    }

    std::vector<int> col_indices;
    for (const auto& attr : attrs) {
        int idx = getColumnIdx(attr);
        if (idx == -1) {
            throw std::runtime_error("Runtime Error in Attribute Table::sortBy(): " + attr + " not found in table");
        }
        col_indices.push_back(idx);
    }

    // 定义比较函数
    auto comparator = [this, &col_indices](const std::vector<std::any>& row1, const std::vector<std::any>& row2) -> bool {
        for (int col_idx : col_indices) {
            // 获取当前比较的值
            int val1 = std::any_cast<int>(row1[col_idx]);
            int val2 = std::any_cast<int>(row2[col_idx]);

            if (val1 != val2) {
                return val1 < val2;
            }
        }
        return false;
    };

    // 对is_null标识符同步调整顺序
    if (!is_null.empty()) {
        // 创建原始索引数组
        std::vector<size_t> indices(data.size());
        std::iota(indices.begin(), indices.end(), 0);       // 索引数组初始为1,2,...,m
        // 根据data的新顺序重新排序indices
        std::stable_sort(indices.begin(), indices.end(), [this, &comparator](size_t i1, size_t i2) {
            return comparator(data[i1], data[i2]);
        });
        // 使用indices重排is_null
        std::vector<std::any> new_is_null(is_null.size());
        for (size_t i = 0; i < indices.size(); i++) {
            new_is_null[i] = is_null[indices[i]];
        }
        is_null = std::move(new_is_null);
    }

    // 执行排序
    std::stable_sort(data.begin(), data.end(), comparator);
}

void Table::setHeaders(const std::vector<std::string>& new_headers) {
    if (!headers.empty()) {
        // 如果表头本不为空，则检查设置的新表头需要与原表头size相同
        if (new_headers.size() != headers.size()) {
            throw std::invalid_argument("Invalid argument in Table::setHeaders(): new headers size doesn't match orign size");
            exit(EXIT_FAILURE);
        }
    }

    this->headers = new_headers;
}

Range Table::getColumnValueRange(int col_idx) const {
    if (this->mode != Table::INT_MODE) {
        throw std::runtime_error("Runtime Error in Table::getColumnValueRange(): only supports INT_MODE table");
    }

    if (data.empty()) {
        throw std::runtime_error("Runtime Error in Table::getColumnValueRange(): Cannot get value range from empty table");
    }

    if (col_idx >= headers.size() || col_idx < 0) {
        throw std::runtime_error("Runtime Error in Table::getColumnValueRange(): Column index out of range");
    }

    // 用第一行初始化范围
    int min_val = std::any_cast<int>(data[0][col_idx]);
    int max_val = min_val;

    // 遍历该列所有的值
    for (const auto& row : data) {
        int val = std::any_cast<int>(row[col_idx]);
        min_val = std::min(min_val, val);
        max_val = std::max(max_val, val);
    }

    return {min_val, max_val};
}

std::vector<Range> Table::getAllColumnValueRange() const {
    if (this->mode != Table::INT_MODE) {
        throw std::runtime_error("Runtime Error in Table::getColumnValueRange(): only supports INT_MODE table");
    }

    if (data.empty()) {
        throw std::runtime_error("Runtime Error in Table::getColumnValueRange(): Cannot get value range from empty table");
    }

    std::vector<Range> ranges;
    ranges.reserve(headers.size());

    for (int i = 0; i < headers.size(); i++) {
        ranges.push_back(getColumnValueRange(i));
    }

    return ranges;
}

void Table::padding(const uint64_t& padding_to) {

    std::vector<Range> value_ranges;

    if (this->data.empty()) {
        for (int i = 0; i < this->headers.size(); i++) {
            value_ranges.push_back({1, MOD - 1});
        }
    } else {
        // 获取所有列的值域
        value_ranges  = getAllColumnValueRange();
    }

    this->padding(padding_to, value_ranges);
}

void Table::padding(const uint64_t& padding_to, std::vector<Range> value_ranges) {
    if (this->mode != Table::INT_MODE) {
        throw std::runtime_error("Runtime Error in Table::padding(): Padding only supports INT_MODE table");
    }

    if (!this->data.empty()) {
        value_ranges = this->getAllColumnValueRange();
    }

    // 使用现代c++随机数生成器
    std::random_device rd;
    std::mt19937 gen(rd());

    // 为每列创建随机数分布
    std::vector<std::uniform_int_distribution<int>> distributions;
    for (const auto& range : value_ranges) {
        distributions.emplace_back(range.first, range.second);
    }
    
    // 先清除数据中原有的dummy
    this->clearDummy();

    // 填充dummy值
    while (this->data.size() < padding_to) {
        std::vector<std::any> dummy_row;
        dummy_row.reserve(this->headers.size());

        for (int col = 0; col < this->headers.size(); col++) {
            dummy_row.push_back(distributions[col](gen));
        }

        this->data.push_back(dummy_row);
        this->is_null.push_back(1);
    }
}

void Table::clearDummy() {
    // 确保当前模式为INT_MODE
    if (mode != INT_MODE) {
        throw std::runtime_error("Runtime Error in Table::clearDummy(): can only be called in INT_MODE.");
    }

    std::vector<std::vector<std::any>> new_data;
    std::vector<std::any> new_is_null;

    for (size_t i = 0; i < data.size(); ++i) {
        try {
            int null_flag = std::any_cast<int>(is_null[i]);
            if (null_flag != 1) { // 保留非dummy数据
                new_data.push_back(std::move(data[i]));
                new_is_null.push_back(std::move(is_null[i]));
            }
        } catch (const std::bad_any_cast &e) {
            throw std::runtime_error("Runtime Error in Table::clearDummy(): Invalid type in is_null array when in INT_MODE.");
        }
    }

    // 更新data和is_null数组
    data = std::move(new_data);
    is_null = std::move(new_is_null);
}

void Table::normalize() {       // 添加对max_freqs的更新
    // 如果没有头部或数据，直接返回
    if (headers.empty() || data.empty()) {
        return;
    }
    
    // 创建原始索引的映射
    std::vector<size_t> indices(headers.size());
    for (size_t i = 0; i < indices.size(); ++i) {
        indices[i] = i;
    }
    
    // 根据headers进行排序
    std::sort(indices.begin(), indices.end(), 
              [this](size_t a, size_t b) { return headers[a] < headers[b]; });
    
    // 创建新的已排序的headers和share_types
    std::vector<std::string> sorted_headers(headers.size());
    std::vector<u_int> sorted_share_types(share_types.size());
    std::vector<uint64_t> sorted_max_freqs(max_freqs.size());
    
    for (size_t i = 0; i < indices.size(); ++i) {
        sorted_headers[i] = headers[indices[i]];
        sorted_share_types[i] = share_types[indices[i]];
        sorted_max_freqs[i] = max_freqs[indices[i]];
    }
    
    // 重新排列data中的每一行
    for (auto& row : data) {
        if (row.size() >= indices.size()) {  // 确保行有足够的元素
            std::vector<std::any> sorted_row(row.size());
            for (size_t i = 0; i < indices.size(); ++i) {
                sorted_row[i] = row[indices[i]];
            }
            row = std::move(sorted_row);
        }
    }
    
    // 用排序后的数据替换原始数据
    headers = std::move(sorted_headers);
    share_types = std::move(sorted_share_types);
    max_freqs = std::move(sorted_max_freqs);
}

void Table::calcualteMaxFreqs() {
    // 如果数据为空，返回空结果
    if (data.empty() || mode != INT_MODE) {
        this->max_freqs.resize(headers.size(), 0);
        return;
    }
    
    // 确定列数（使用第一行的元素数量）
    size_t numColumns = data[0].size();
    std::vector<uint64_t> maxFrequencies(numColumns, 0);
    
    // 对每一列进行处理
    for (size_t col = 0; col < numColumns; ++col) {
        // 使用unordered_map统计频率，键为int值，值为出现次数
        std::unordered_map<int, uint64_t> frequencyMap;
        
        // 统计该列中每个值的频率
        for (const auto& row : data) {
            // 检查列索引是否有效
            if (col < row.size()) {
                try {
                    int value = std::any_cast<int>(row[col]);
                    frequencyMap[value]++;
                } catch (const std::bad_any_cast&) {
                    // 如果类型转换失败，可以选择忽略或记录错误
                    std::cerr << "Warning: Value at row " << (&row - &data[0]) 
                              << ", column " << col << " is not an int." << default_color << std::endl;
                }
            }
        }
        
        // 找出该列中频率最高的值
        uint64_t maxFreq = 0;
        for (const auto& pair : frequencyMap) {
            if (pair.second > maxFreq) {
                maxFreq = pair.second;
            }
        }
        
        // 存储该列的最大频率
        maxFrequencies[col] = maxFreq;
    }
    
    this->max_freqs = maxFrequencies;
}

void Table::addNoiseToMFs() {
    int noise = genIntegerNoise(1, epsilon, ONESIDED_LAPLACE_MECH);
    
    for (auto& mf : max_freqs) {
        mf += noise;
    }
}

void log(const std::string& log) {
    if (!enable_log) { return; }
    std::lock_guard<std::mutex> guard(table_print_mutex);
    std::cout << print_color << log << default_color << std::endl;
}

void log_process(size_t current, size_t total) {
    if (!enable_log) { return; }

    if (total == 0) return;

    // 计算当前百分比（取整）
    int currentPrecentage = static_cast<int>((current * 100.0) / total);

    // 静态变量用于记录上一次输出的百分比，保证在同一个循环中同一百分比只打印一次
    static int lastPrintedPercentage = -1;

    if (currentPrecentage != lastPrintedPercentage) {
        lastPrintedPercentage = currentPrecentage;
        std::cout << "\r" << print_color << currentPrecentage << "% complete" << default_color << std::flush;  
    }

    if (current == total) {
        std::cout << std::endl;
    }
}

Table Join(const Table& table1, const Table& table2) {
    // 检查待连接的表是否都为INT_MODE，否则不提供连接操作
    if (!(table1.mode == Table::INT_MODE && table2.mode == Table::INT_MODE)) {
        throw std::runtime_error("RunTime error in Table.cpp Join(): Table mode must be INT_MODE");
    }

    Table result(Table::INT_MODE);

    // 查找两个表中相同的headers作为连接键
    std::vector<std::pair<size_t, size_t>> keyIndices;
    std::vector<std::string> commonHeaders;
    for (size_t i = 0; i < table1.headers.size(); ++i) {
        for (size_t j = 0; j < table2.headers.size(); ++j) {
            if (table1.headers[i] == table2.headers[j]) {
                keyIndices.emplace_back(i, j);
                commonHeaders.push_back(table1.headers[i]);
                break;
            }
        }
    }

    if (keyIndices.empty()) {
        throw std::runtime_error("RunTime error in Table.cpp Join(): No common headers found between tables");
    }

    // 构建结果表的表头: result.headers = [table1.headers] + ([table2.headers] \ [commonHeaders])
    result.headers = table1.headers;
    for (const auto& h : table2.headers) {
        if (std::find(commonHeaders.begin(), commonHeaders.end(), h) == commonHeaders.end()) {
            result.headers.push_back(h);
        }
    }

    // 构建结果表的共享类型
    result.share_types = std::vector<u_int>(result.headers.size(), NO_SHARING);

    // 新增 构建结果表的最大频率
    // 取出 table1 连接键的 max_freqs
    std::vector<uint64_t> t1_key_max_freqs;
    for (const auto& pair : keyIndices) {
        t1_key_max_freqs.push_back(table1.max_freqs[pair.first]);
    }
    uint64_t t1_min_key_mfs = *std::min_element(t1_key_max_freqs.begin(), t1_key_max_freqs.end());
    // 取出 table2 连接键的 max_freqs
    std::vector<uint64_t> t2_key_max_freqs;
    for (const auto& pair : keyIndices) {
        t2_key_max_freqs.push_back(table2.max_freqs[pair.second]);
    }
    uint64_t t2_min_key_mfs = *std::min_element(t2_key_max_freqs.begin(), t2_key_max_freqs.end());

    // 用 t2_min_key_mfs 更新结果表中 table1 中属性的 max_freqs
    for (const auto& mf : table1.max_freqs) {
        result.max_freqs.push_back(mf * t2_min_key_mfs);
    }
    // 用 t1_min_key_mfs 更新结果表中 table2 中属性的 max_freqs
    for (int i = 0; i < table2.max_freqs.size(); i++) {
        if (std::find(commonHeaders.begin(), commonHeaders.end(), table2.headers[i]) == commonHeaders.end()) {
            result.max_freqs.push_back(table2.max_freqs[i] * t1_min_key_mfs);
        }
    }

    // 打印连接表的相关信息
    std::string t1_header_str;
    std::string t2_header_str;
    for (int i = 0; i < table1.headers.size(); i++) {
        if (i == 0) {
            t1_header_str += "(";
        }
        t1_header_str += table1.headers[i];
        if (i != table1.headers.size() - 1) {
            t1_header_str += ", ";
        } else {
            t1_header_str += ")";
        }
    }
    for (int i = 0; i < table2.headers.size(); i++) {
        if (i == 0) {
            t2_header_str += "(";
        }
        t2_header_str += table2.headers[i];
        if (i != table2.headers.size() - 1) {
            t2_header_str += ", ";
        } else {
            t2_header_str += ")";
        }
    }
    log("T1" + t1_header_str + " ⋈ T2" + t2_header_str + ", |T1| = " + std::to_string(table1.data.size()) + ", |T2| = " + std::to_string(table2.data.size()) + ", Loop count = " + std::to_string(table1.data.size() * table2.data.size()));

    // 对每一行数据进行连接
    for (int i = 0; i < table1.data.size(); i++) {
        int isNull1 = std::any_cast<int>(table1.is_null[i]);
        // if (isNull1 == 1) continue;
        for (int j = 0; j < table2.data.size(); j++) {
            int isNull2 = std::any_cast<int>(table2.is_null[j]);
            // if (isNull2 == 1) continue;

            bool match = true;
            // 检查所有连接键是否匹配
            for (const auto &[idx1, idx2] : keyIndices) {
                int val1 = std::any_cast<int>(table1.data[i][idx1]);
                int val2 = std::any_cast<int>(table2.data[j][idx2]);
                if (val1 != val2) {
                    match = false;
                    break;
                }
            }

            // 如果所有键都匹配，合并这两行
            if (match && isNull1 == 0 && isNull2 == 0) {
                std::vector<std::any> newRow = table1.data[i];
                for (size_t k = 0; k < table2.headers.size(); ++k) {
                    if (std::find(commonHeaders.begin(), commonHeaders.end(), table2.headers[k]) ==
                        commonHeaders.end()) {
                        newRow.push_back(table2.data[j][k]);
                    }
                }
                result.data.push_back(std::move(newRow));
                result.is_null.push_back(0);
            }
        }

        log_process(i + 1, table1.data.size());
    }

    return result;
}

Table Join(const Table& table1, const Table& table2, const uint64_t& padding_to) {
    Table result = Join_Hash(table1, table2);

    if (padding_to == 0) {
        return result;
    } else if (padding_to == CARTESIAN_BOUND) {
        result.padding(table1.data.size() * table2.data.size());
        std::cout << print_color << "Use CARTESIAN_BOUND Padding Strategy: CARTESIAN_BOUND = " << table1.data.size() * table2.data.size() << default_color << std::endl;
    } else if (padding_to == AGM_BOUND) {
        // 确保AGM Bound已经计算，否则填充到笛卡尔积
        if (AGM_BOUND_ == CARTESIAN_BOUND) {
            result.padding(table1.data.size() * table2.data.size());
            std::cout << print_color
                      << "Use CARTESIAN_BOUND Padding Strategy due to AGM has not been initialized: CARTESIAN_BOUND = "
                      << table1.data.size() * table2.data.size() << default_color << std::endl;
        } else {
            result.padding(std::max(AGM_BOUND_, result.data.size()));
            std::cout << print_color << "Use AGM_BOUND Padding Strategy: AGM Bound = " << AGM_BOUND_ << default_color
                      << std::endl;
        }
    } else if (padding_to == MF_BOUND || padding_to == MIX_AGM_MF_BOUND) {
        // 计算两表连接键的索引
        std::vector<std::pair<size_t, size_t>> keyIndices;
        for (size_t i = 0; i < table1.headers.size(); ++i) {
            for (size_t j = 0; j < table2.headers.size(); ++j) {
                if (table1.headers[i] == table2.headers[j]) {
                    keyIndices.emplace_back(i, j);
                    break;
                }
            }
        }
        // 获取连接键的最大频率信息
        std::vector<std::vector<uint64_t>> MFs;
        MFs.reserve(keyIndices.size());
        for (const auto &key_pair : keyIndices) {
            MFs.push_back({table1.max_freqs[key_pair.first], table2.max_freqs[key_pair.second]});
        }
        // 计算当前连接的agm bound和mf bound
        auto current_agm_bound = Utils::computeAGMBound(table1.headers, table2.headers, table1.data.size(), table2.data.size());
        auto current_mf_bound = Utils::computeMFBound(MFs, {table1.data.size(), table2.data.size()});

        if (padding_to == MF_BOUND) {
            result.padding(current_mf_bound);
            log("Use MF_BOUND Padding Strategy: MF Bound = " + std::to_string(current_mf_bound));
        } else if (padding_to == MIX_AGM_MF_BOUND) {
            result.padding(std::min({AGM_BOUND_, current_agm_bound, current_mf_bound}));
            log("Use MIX_AGM_MF_BOUND Padding Strategy: MF Bound = " + std::to_string(current_mf_bound) + ", Global AGM Bound = " + std::to_string(AGM_BOUND_) + ", Current AGM Bound = " + std::to_string(current_agm_bound));
        }
    } else if (padding_to == MIX_BOUND) {
        result.padding(MIX_BOUND_);
        log("Use MIX_BOUND Padding Strategy: Mix Bound = " + std::to_string(MIX_BOUND_));
    } else {
        result.padding(std::max(padding_to, result.data.size()));
        log("Use Determined Value Padding Strategy: padding_to = " + std::to_string(padding_to));
    }

    return result;
}

Table Join_Hash(const Table& table1, const Table& table2) {
    // 检查待连接的表是否都为INT_MODE，否则不提供连接操作
    if (!(table1.mode == Table::INT_MODE && table2.mode == Table::INT_MODE)) {
        throw std::runtime_error("RunTime error in Table.cpp Join(): Table mode must be INT_MODE");
    }

    Table result(Table::INT_MODE);

    // 查找两个表中相同的headers作为连接键
    std::vector<std::pair<size_t, size_t>> keyIndices;
    std::vector<std::string> commonHeaders;
    for (size_t i = 0; i < table1.headers.size(); ++i) {
        for (size_t j = 0; j < table2.headers.size(); ++j) {
            if (table1.headers[i] == table2.headers[j]) {
                keyIndices.emplace_back(i, j);
                commonHeaders.push_back(table1.headers[i]);
                break;
            }
        }
    }

    if (keyIndices.empty()) {
        throw std::runtime_error("RunTime error in Table.cpp Join(): No common headers found between tables");
    }

    // 区分大小表
    size_t n1 = table1.data.size();
    size_t n2 = table2.data.size();

    const Table* smaller_table = (n1 <= n2) ? &table1 : &table2;
    const Table* bigger_table = (n1 <= n2) ? &table2 : &table1;

    // 打印连接表的相关信息
    std::string smaller_header_str;
    std::string bigger_header_str;
    for (int i = 0; i < smaller_table->headers.size(); i++) {
        if (i == 0) {
            smaller_header_str += "(";
        }
        smaller_header_str += smaller_table->headers[i];
        if (i != smaller_table->headers.size() - 1) {
            smaller_header_str += ", ";
        } else {
            smaller_header_str += ")";
        }
    }
    for (int i = 0; i < bigger_table->headers.size(); i++) {
        if (i == 0) {
            bigger_header_str += "(";
        }
        bigger_header_str += bigger_table->headers[i];
        if (i != bigger_table->headers.size() - 1) {
            bigger_header_str += ", ";
        } else {
            bigger_header_str += ")";
        }
    }

    log("T1" + bigger_header_str + " ⋈ T2" + smaller_header_str + ", |T1| = " + std::to_string(bigger_table->data.size()) 
        + " as probing table, |T2| = " + std::to_string(smaller_table->data.size()) + " as building table");

    // 构建结果表的表头: result.headers = [bigger.headers] + ([smaller.headers] \ [commonHeaders])
    result.headers = bigger_table->headers;
    for (const auto& h : smaller_table->headers) {
        if (std::find(commonHeaders.begin(), commonHeaders.end(), h) == commonHeaders.end()) {
            result.headers.push_back(h);
        }
    }

    // 构建结果表的共享类型
    result.share_types = std::vector<u_int>(result.headers.size(), NO_SHARING);

    // 新增 构建结果表的最大频率    TODO
    // 取出两表连接键的mfs
    std::vector<uint64_t> smaller_key_mfs;
    std::vector<uint64_t> bigger_key_mfs;
    for (const auto& pair : keyIndices) {
        if (n1 <= n2) {     // smaller -> t1, bigger -> t2
            smaller_key_mfs.push_back(smaller_table->max_freqs[pair.first]);
            bigger_key_mfs.push_back(bigger_table->max_freqs[pair.second]);
        } else {            // bigger -> t1, smaller -> t2
            smaller_key_mfs.push_back(smaller_table->max_freqs[pair.second]);
            bigger_key_mfs.push_back(bigger_table->max_freqs[pair.first]);
        }
        
    }

    // 计算连接键的最小mf
    uint64_t smaller_key_min_mf = *std::min_element(smaller_key_mfs.begin(), smaller_key_mfs.end());
    uint64_t bigger_key_min_mf = *std::min_element(bigger_key_mfs.begin(), bigger_key_mfs.end());
    
    // 构建结果表的mfs: result.headers = [bigger.headers] + ([smaller.headers] \ [commonHeaders])
    // 用 smaller 的 key 中最小的 mf 更新结果表中 bigger 中属性的 max_freqs
    for (const auto& mf : bigger_table->max_freqs) {
        result.max_freqs.push_back(mf * smaller_key_min_mf);
    }
    // 用 bigger 的 key 中最小的 mf 更新结果表中 smaller 中属性的 max_freqs
    for (int i = 0; i < smaller_table->max_freqs.size(); i++) {
        if (std::find(commonHeaders.begin(), commonHeaders.end(), smaller_table->headers[i]) == commonHeaders.end()) {
            result.max_freqs.push_back(smaller_table->max_freqs[i] * bigger_key_min_mf);
        }
    }

    // 初始化哈希表 (keys_str, row)
    std::unordered_map<std::string, std::vector<const std::vector<std::any>*>> hash_table;
    // log("Building hash table");
    for (int i = 0; i < smaller_table->data.size(); i++) {
        std::string keys;
        for (const auto& pair : keyIndices) {
            size_t idx = (n1 <= n2) ? pair.first : pair.second;
            auto key = std::any_cast<int>(smaller_table->data[i][idx]);
            keys += (std::to_string(key) + " ");
        }
        int isNull = std::any_cast<int>(smaller_table->is_null[i]);
        if (isNull == 0) {
            hash_table[keys].push_back(&smaller_table->data[i]);
        }
    }

    // log("Probing hash table");
    for (int i = 0; i < bigger_table->data.size(); i++) {
        int isNull = std::any_cast<int>(bigger_table->is_null[i]);

        std::string bigger_keys;
        for (const auto& pair : keyIndices) {
            size_t idx = (n1 <= n2) ? pair.second : pair.first;
            auto key = std::any_cast<int>(bigger_table->data[i][idx]);
            bigger_keys += (std::to_string(key) + " ");
        }
        if (hash_table.count(bigger_keys) && isNull == 0) {
            for (int j = 0; j < hash_table[bigger_keys].size(); j++) {
                std::vector<std::any> joined_row = bigger_table->data[i];
                for (int k = 0; k < smaller_table->headers.size(); k++) {
                    if (std::find(commonHeaders.begin(), commonHeaders.end(), smaller_table->headers[k]) ==
                        commonHeaders.end()) {
                        joined_row.push_back((*hash_table[bigger_keys][j])[k]);
                    }
                }
                result.data.push_back(std::move(joined_row));
                result.is_null.push_back(0);
            }
        }

        // log_process(i + 1, bigger_table->data.size());
    }

    return result;
}

std::pair<Table, Index> BucketJoin(const Table& table1, const Table& table2, const Index& table1_idx, const Index& table2_idx, const uint64_t& padding_to) {
    // 检查待连接的表是否都为INT_MODE，否则不提供连接操作
    if (!(table1.mode == Table::INT_MODE && table2.mode == Table::INT_MODE)) {
        throw std::runtime_error("RunTime error in Table.cpp BucketJoin(): Table mode must be INT_MODE");
    }

    // 验证两个表的索引属性集相同
    if (table1_idx.getAttrs() != table2_idx.getAttrs()) {
        throw std::runtime_error("RunTime error in Table.cpp BucketJoin(): Table indices must have same attributes");
    }

    // 检查两表索引的ranges是否相同
    if (table1_idx.getRanges() != table2_idx.getRanges()) {
        throw std::runtime_error("RunTime error in Table.cpp BucketJoin(): Table indices must have same Ranges");
    }
    
    // 检查两表索引的widths是否相同
    if (table1_idx.getWidths() != table2_idx.getWidths()) {
        throw std::runtime_error("RunTime error in Table.cpp BucketJoin(): Table indices must have same widths");
    }

    // 计算两表连接键的索引
    std::vector<std::pair<size_t, size_t>> keyIndices;
    for (size_t i = 0; i < table1.headers.size(); ++i) {
        for (size_t j = 0; j < table2.headers.size(); ++j) {
            if (table1.headers[i] == table2.headers[j]) {
                keyIndices.emplace_back(i, j);
                break;
            }
        }
    }
    // 获取连接键的最大频率信息
    std::vector<std::vector<uint64_t>> MFs;
    MFs.reserve(keyIndices.size());
    for (const auto& key_pair : keyIndices) {
        MFs.push_back({table1.max_freqs[key_pair.first], table2.max_freqs[key_pair.second]});
    }

    // 为并行处理准备存储结构
    int bucket_count = table1_idx.getStarts().size();
    
    // 使用指针数组避免Table的拷贝构造问题
    std::vector<std::unique_ptr<Table>> bucket_results(bucket_count);
    std::vector<bool> bucket_has_result(bucket_count, false);

    // 并行处理所有桶
    #pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < bucket_count; i++) {
        try {
            // 提取桶对应区域的数据
            Table bucket1 = table1.slice(table1_idx.getStarts()[i], table1_idx.getEnds()[i]);
            Table bucket2 = table2.slice(table2_idx.getStarts()[i], table2_idx.getEnds()[i]);

            // 执行桶内连接
            // #pragma omp critical(log_section)
            // {
            //     log("Join Bucket " + std::to_string(i) + "...");
            // }
            
            Table bucket_result = Join_Hash(bucket1, bucket2);
            bucket_result.normalize();

            // 计算padding的值域范围
            std::vector<Range> value_ranges;
            std::vector<std::string> key_attr = table1_idx.getAttrs();
            for (const auto& header : bucket_result.headers) {
                bool isKey = false;
                for (size_t idx = 0; idx < key_attr.size(); idx++) {
                    if (header == key_attr[idx]) {
                        value_ranges.push_back(table1_idx.getRanges(i)[idx]);
                        isKey = true;
                        break;
                    }
                }
                if (!isKey) {
                    value_ranges.push_back({1, MOD - 1});
                }
            }

            // 处理padding
            if (padding_to != 0) {
                if (padding_to == CARTESIAN_BOUND) {
                    bucket_result.padding(bucket1.data.size() * bucket2.data.size(), value_ranges);
                } else if (padding_to == MF_BOUND || padding_to == AGM_BOUND || padding_to == MIX_AGM_MF_BOUND) {
                    auto current_agm_bound = Utils::computeAGMBound(bucket1.headers, bucket2.headers, 
                                                                  bucket1.data.size(), bucket2.data.size());
                    auto current_mf_bound = Utils::computeMFBound(MFs, {bucket1.data.size(), bucket2.data.size()});

                    if (padding_to == MF_BOUND) {
                        bucket_result.padding(current_mf_bound, value_ranges);
                    } else if (padding_to == AGM_BOUND) {
                        if (AGM_BOUND_ == CARTESIAN_BOUND) {
                            bucket_result.padding(bucket1.data.size() * bucket2.data.size(), value_ranges);
                        } else {
                            bucket_result.padding(std::min(AGM_BOUND_, current_agm_bound), value_ranges);
                            #pragma omp critical(log_section)
                            {
                                log("Use AGM_BOUND Padding Strategy: Global AGM Bound = " + std::to_string(AGM_BOUND_) +
                                    ", Current AGM Bound = " + std::to_string(current_agm_bound));
                            }
                        }
                    } else if (padding_to == MIX_AGM_MF_BOUND) {
                        bucket_result.padding(std::min({current_agm_bound, AGM_BOUND_, current_mf_bound}), value_ranges);
                        #pragma omp critical(log_section)
                        {
                            log("Use MIX_AGM_MF_BOUND Padding Strategy: MF Bound = " + std::to_string(current_mf_bound) +
                                ", Global AGM Bound = " + std::to_string(AGM_BOUND_) +
                                ", Current AGM Bound = " + std::to_string(current_agm_bound));
                        }
                    }
                } else if (padding_to == MIX_BOUND) {
                    // 重构出星型结构
                    std::vector<std::vector<int>> edges;
                    // 重构对称节点集
                    std::vector<int> leaves;
                    // 确定 root
                    int root = std::stoi(table1_idx.getAttrs()[0]);
                    // 读取 table1 表示的星型的边
                    for (auto leaf_str : table1.headers) {
                        int leaf = std::stoi(leaf_str);
                        if (leaf != root) {
                            edges.push_back(std::vector<int>({root, leaf}));
                            leaves.push_back(leaf);
                        }
                    }
                    // 读取 table2 表示的星型的边
                    for (auto leaf_str : table2.headers) {
                        int leaf = std::stoi(leaf_str);
                        if (leaf != root) {
                            edges.push_back(std::vector<int>({root, leaf}));
                            leaves.push_back(leaf);
                        }
                    }
                    // 计算 mixed bound
                    auto mixed_bound = Utils::mixedUpperBound(edges, bucket2.data.size(), bucket2.max_freqs[0], { leaves });
                    bucket_result.padding(mixed_bound, value_ranges);
                    #pragma omp critical(log_section)
                    {
                        log("Use MIX_BOUND Padding Strategy: Mix Bound = " + std::to_string(mixed_bound));
                    }
                } else {
                    bucket_result.padding(std::max(padding_to, bucket_result.data.size()), value_ranges);
                }
            }

            // 存储结果 - 使用unique_ptr避免拷贝问题
            if (!bucket_result.data.empty()) {
                bucket_results[i] = std::make_unique<Table>(std::move(bucket_result));
                bucket_has_result[i] = true;
            }
        } catch (const std::exception& e) {
            #pragma omp critical(error_section)
            {
                std::cerr << "Error processing bucket " << i << ": " << e.what() << std::endl;
            }
        }
    }

    // 串行合并结果
    Table result(Table::INT_MODE);
    std::vector<int> result_starts;
    std::vector<int> result_ends;
    std::vector<std::pair<std::string, int>> result_intervals;
    int current_pos = 0;

    for (int i = 0; i < bucket_count; i++) {
        if (bucket_has_result[i] && bucket_results[i]) {
            result_starts.push_back(current_pos);
            current_pos += bucket_results[i]->data.size();
            result_ends.push_back(current_pos);
            
            auto interval = table1_idx.getSortedIntervals()[i];
            interval.second = bucket_results[i]->data.size();
            result_intervals.push_back(interval);

            if (result.data.empty()) {
                result = std::move(*bucket_results[i]);
            } else {
                result.concate(*bucket_results[i]);
            }
        }
    }

    // // 初始化结果表
    // Table result(Table::INT_MODE);

    // // 用于记录结果表中每个桶的起始和结束位置
    // std::vector<int> result_starts;
    // std::vector<int> result_ends;
    // std::vector<std::pair<std::string, int>> result_intervals;
    // int current_pos = 0;                    // 记录结果表中当前处理到的位置

    // // 分桶处理
    // for (int i = 0; i < table1_idx.getStarts().size(); i++) {
    //     // 提取桶对应区域的数据
    //     Table bucket1 = table1.slice(table1_idx.getStarts()[i], table1_idx.getEnds()[i]);
    //     Table bucket2 = table2.slice(table2_idx.getStarts()[i], table2_idx.getEnds()[i]);

    //     // 执行桶内连接
    //     log("Join Bucket " + std::to_string(i) + "...");
    //     Table bucket_result = Join_Hash(bucket1, bucket2);
    //     // 调整表头
    //     bucket_result.normalize();

    //     // 计算padding中各值的填充范围，保证连接键的范围在bucket中，其他列任意
    //     std::vector<Range> value_ranges;
    //     std::vector<std::string> key_attr = table1_idx.getAttrs();
    //     for (auto header : bucket_result.headers) {
    //         // 判断是否为key
    //         bool isKey = false;
    //         for (int idx = 0; idx < key_attr.size(); idx++) {
    //             if (header == key_attr[idx]) {
    //                 value_ranges.push_back(table1_idx.getRanges(i)[idx]);    // bucket i 的值域
    //                 isKey = true;
    //                 break;
    //             }
    //         }
    //         if (!isKey) {
    //             value_ranges.push_back({1, MOD - 1});
    //         }
    //     }

    //     if (padding_to != 0) {
    //         if (padding_to == CARTESIAN_BOUND) {
    //             // 填充到笛卡尔积大小
    //             bucket_result.padding(bucket1.data.size() * bucket2.data.size(), value_ranges);
    //         } else if (padding_to == MF_BOUND || padding_to == AGM_BOUND || padding_to == MIX_AGM_MF_BOUND) {
    //             AGM_BOUND_ = Utils::computeStarAGMBound(bucket1.headers.size(), bucket2.data.size());
    //             auto current_agm_bound =
    //                 Utils::computeAGMBound(bucket1.headers, bucket2.headers, bucket1.data.size(), bucket2.data.size());
    //             auto current_mf_bound = Utils::computeMFBound(MFs, {bucket1.data.size(), bucket2.data.size()});

    //             if (padding_to == MF_BOUND) {
    //                 // 填充到MF Bound需要计算此次桶连接的预估上界
    //                 bucket_result.padding(current_mf_bound, value_ranges);
    //             } else if (padding_to == AGM_BOUND) {
    //                 // 确保AGM Bound已经计算，否则使用笛卡尔积上界
    //                 if (AGM_BOUND_ == CARTESIAN_BOUND) {
    //                     bucket_result.padding(bucket1.data.size() * bucket2.data.size(), value_ranges);
    //                 } else {
    //                     bucket_result.padding(std::min(AGM_BOUND_, current_agm_bound), value_ranges);
    //                     log("Use AGM_BOUND Padding Strategy: Global AGM Bound = " + std::to_string(AGM_BOUND_) +
    //                         ", Current AGM Bound = " + std::to_string(current_agm_bound));
    //                 }
    //             } else if (padding_to == MIX_AGM_MF_BOUND) {
    //                 // 填充到AGM或MF中更小的界限
    //                 bucket_result.padding(std::min({current_agm_bound, AGM_BOUND_, current_mf_bound}), value_ranges);
    //                 log("Use MIX_AGM_MF_BOUND Padding Strategy: MF Bound = " + std::to_string(current_mf_bound) +
    //                     ", Global AGM Bound = " + std::to_string(AGM_BOUND_) +
    //                     ", Current AGM Bound = " + std::to_string(current_agm_bound));
    //             }
    //         } else {
    //             // 填充到指定大小
    //             bucket_result.padding(std::max(padding_to, bucket_result.data.size()), value_ranges);
    //         }
    //     }

    //     // 如果桶内连接结果非空，更新结果索引
    //     if (!bucket_result.data.empty()) {
    //         // 更新起止位置
    //         result_starts.push_back(current_pos);
    //         current_pos += bucket_result.data.size();
    //         result_ends.push_back(current_pos);
    //         // 更新区间对应频次计数
    //         auto interval = table1_idx.getSortedIntervals()[i];
    //         interval.second = bucket_result.data.size();
    //         result_intervals.push_back(interval);
    //     }

    //     // 将桶内结果添加到最终结果表
    //     if (result.data.empty()) {
    //         result = bucket_result;
    //     } else {
    //         result.concate(bucket_result);
    //     }
    // }

    Index result_idx(table1_idx.getAttrs(), table1_idx.getRanges(), table1_idx.getWidths(), result_starts, result_ends, result_intervals);

    return {result, result_idx};
}

std::vector<std::string> getJoinKey(const Table& table1, const Table& table2) {
    // 查找两个表中相同的headers作为连接键
    std::vector<std::string> commonHeaders;
    for (size_t i = 0; i < table1.headers.size(); ++i) {
        for (size_t j = 0; j < table2.headers.size(); ++j) {
            if (table1.headers[i] == table2.headers[j] && table1.headers[i] != "isNull") {
                commonHeaders.push_back(table1.headers[i]);
                break;
            }
        }
    }

    return commonHeaders;
}