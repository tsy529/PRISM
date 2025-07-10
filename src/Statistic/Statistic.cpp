#include "Statistic.h"
/*
Bucket::Bucket(const std::vector<std::string>& attrs) {
    this->attrs = attrs;
    int attr_count = attrs.size();
    this->ranges.resize(attr_count);
    this->count = 0;
}

int Bucket::getAttrId(const std::string& attr) {
    auto it = std::find(attrs.begin(), attrs.end(), attr);
    return it != attrs.end() ? it - attrs.begin() : -1;
}

int Bucket::getLowValue(const std::string& attr) {
    int i = getAttrId(attr);
    if (i == -1) {
        throw std::runtime_error("Runtime Error in getLowValue: No such attr " + attr);
    }
    return ranges[i].first;
}

int Bucket::getHighValue(const std::string& attr) {
    int i = getAttrId(attr);
    if (i == -1) {
        throw std::runtime_error("Runtime Error in getHighValue: No such attr " + attr);
    }
    return ranges[i].second;
}

void Bucket::setLowValue(const std::string& attr, int value) {
    int i = getAttrId(attr);
    if (i == -1) {
        throw std::runtime_error("Runtime Error in setLowValue: No such attr " + attr);
    }
    this->ranges[i].first = value;
}

void Bucket::setHighValue(const std::string& attr, int value) {
    int i = getAttrId(attr);
    if (i == -1) {
        throw std::runtime_error("Runtime Error in setHighValue: No such attr " + attr);
    }
    this->ranges[i].second = value;
}

bool Bucket::operator<(Bucket& other) {
    for (const auto& attr : this->attrs) {
        // 按照attrs中的顺序作为比较的优先级
        int low1 = this->getLowValue(attr);
        int low2 = other.getLowValue(attr);
        if (low1 != low2) {
            return low1 < low2;
        }
    }
    return false;
}

Index::Index(Histogram histogram) {
    *this = histogram.generateIndex();
}

std::string Index::toString() const {
    std::ostringstream oss;
    for (int i = 0; i < ranges.size(); i++) {
        oss << "{ ";
        for (int j = 0; j < attrs.size(); j++) {
            oss << attrs[j] << "[" << ranges[i][j].first << "," << ranges[i][j].second << ") ";
        }
        oss << "} -> start=" << starts[i] << ", end=" << ends[i] << "\n";
    }
    oss << "Max Frequency: " << this->max_frequency << "\n";
    return oss.str();
}

void Index::addNoise() {
    int max_pos = ends.back();

    for (int i = 0; i < starts.size(); i++) {
        starts[i] = std::max(0, starts[i] - genIntegerNoise(1.0, epsilon, TRUNC_LAPLACE_MECH));
        ends[i] = std::min(max_pos, ends[i] + genIntegerNoise(1.0, epsilon, TRUNC_LAPLACE_MECH));
    }
}

Index Index::reduction(std::string attr) {
    // 找到指定属性在attrs中的位置
    int attrPos = -1;
    for (int i = 0; i < attrs.size(); i++) {
        if (attrs[i] == attr) {
            attrPos = i;
            break;
        }
    }
    
    // 如果找不到指定属性,返回原索引的复制
    if (attrPos == -1) {
        return *this;
    }

    // 创建新的一维索引
    Index reducedIndex;
    reducedIndex.attrs = {attr}; // 只保留指定的属性
    
    // 使用map按照要降维的属性区间进行分组
    std::map<Range, std::vector<int>> bucketGroups;
    for (int i = 0; i < ranges.size(); i++) {
        Range attrRange = ranges[i][attrPos];
        bucketGroups[attrRange].push_back(i);
    }
    
    // 对每个区间计算新的starts和ends
    for (const auto& group : bucketGroups) {
        // 添加新的区间
        std::vector<Range> newRange = {group.first};
        reducedIndex.ranges.push_back(newRange);
        
        // 计算该区间的实际start位置
        int startCount = 0;
        for (const auto& bucket : bucketGroups) {
            if (bucket.first < group.first) {
                // 对所有小于当前区间的bucket，累加其最大可能的数据量
                for (int bucketId : bucket.second) {
                    startCount += (ends[bucketId] - starts[bucketId]);
                }
            }
        }
        
        // 计算该区间的实际end位置
        int endCount = startCount;
        for (int bucketId : group.second) {
            endCount += (ends[bucketId] - starts[bucketId]);
        }
        
        // 添加新的starts和ends
        reducedIndex.starts.push_back(startCount);
        reducedIndex.ends.push_back(endCount);
    }
    
    return reducedIndex;
}

Histogram::Histogram(std::string attr, std::vector<int> &records, Range range, int width) {
    this->attrs.push_back(attr);
    this->widths.push_back(width);
    this->ranges.push_back(range);
    this->total_count = records.size();

    // 初始化buckets
    int current_start = range.first;
    while (current_start < range.second) {
        int current_end = std::min(current_start + width, range.second);
        Bucket bucket({attr});
        bucket.ranges[0] = {current_start, current_end};
        this->buckets.push_back(bucket);
        current_start += width;
    }

    // 一趟扫描，统计每个区间的计数
    for (int x : records) {
        if (x < range.first || x >= range.second) continue;      // 忽略不在区间内的元素
        int delta = x - range.first;
        int i = delta / width;
        this->buckets[i].count++;
    }

    // 统计最大频率
    findMaxFrequency({records});
}

Histogram::Histogram(std::vector<std::string> &attrs, std::vector<std::vector<int>>& records, std::vector<Range>& ranges, std::vector<int>& widths) {
    this->attrs = attrs;
    this->widths = widths;
    this->ranges = ranges;
    this->total_count = records.size();

    // 计算每列的划分数
    std::vector<int> divide_num(attrs.size(), 0);
    for (int i = 0; i < attrs.size(); i++) {
        divide_num[i] = ceil(double(ranges[i].second - ranges[i].first) / widths[i]);
    }

    // 初始化Buckets
    Bucket init_bucket(attrs);
    this->buckets.push_back(init_bucket);
    for (int attr_idx = 0; attr_idx < attrs.size(); attr_idx++) {
        std::vector<Bucket> new_buckets;
        for (const auto& bucket : this->buckets) {
            int low_value = ranges[attr_idx].first;
            int high_value;
            for (int i = 0; i < divide_num[attr_idx]; i++) {
                high_value = std::min(low_value + widths[attr_idx], ranges[attr_idx].second);
                Bucket new_bucket = bucket;
                new_bucket.setLowValue(attrs[attr_idx], low_value);
                new_bucket.setHighValue(attrs[attr_idx], high_value);
                new_buckets.push_back(new_bucket);
                low_value = high_value;
            }
        }
        this->buckets = new_buckets;
    }

    // 遍历每条记录
    for (const auto& record : records) {
        // 检查记录落在哪个桶中
        for (int bi = 0; bi < buckets.size(); bi++) {
            bool falls_in_bucket = true;
            // 检查记录的每个属性值是否在桶的范围内
            for (size_t i = 0; i < attrs.size(); ++i) {
                int value = record[i];
                int low = buckets[bi].getLowValue(attrs[i]);
                int high = buckets[bi].getHighValue(attrs[i]);
                
                if (value < low || value >= high) {
                    falls_in_bucket = false;
                    break;
                }
            }
            // 如果记录落在这个桶中，增加计数
            if (falls_in_bucket) {
                buckets[bi].count++;
            }
        }
    }

    // 统计最大频率
    findMaxFrequency(records);
}

void Histogram::findMaxFrequency(const std::vector<std::vector<int>>& records) {
    if (records.empty()) {
        this->max_frequency = 0;
    }
    
    // 定义自定义哈希表来统计行频率
    // 使用lambda表达式作为哈希函数
    auto vectorHash = [](const std::vector<int>& v) -> size_t {
        std::hash<int> hasher;
        size_t seed = 0;
        for (int i : v) {
            seed ^= hasher(i) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    };
    
    // 使用lambda表达式作为相等比较函数
    auto vectorEqual = [](const std::vector<int>& lhs, const std::vector<int>& rhs) -> bool {
        return lhs == rhs;
    };
    
    // 创建带有自定义哈希和相等函数的unordered_map
    std::unordered_map<std::vector<int>, int, 
                      decltype(vectorHash), 
                      decltype(vectorEqual)> frequency(10, vectorHash, vectorEqual);
    
    // 统计每行出现的频率
    for (const auto& row : records) {
        frequency[row]++;
    }
    
    // 找出最大频率
    int max_frequency = 0;
    for (const auto& pair : frequency) {
        max_frequency = std::max(max_frequency, pair.second);
    }
    
    this->max_frequency = max_frequency;
}

Histogram Histogram::reduction(std::string attr) {
    // 创建新的一维直方图
    std::vector<int> empty;
    Histogram projected(attr, empty, ranges[getAttrId(attr)], widths[getAttrId(attr)]);
    projected.total_count = this->total_count;

    // 合并计数
    std::vector<int> count(projected.buckets.size(), 0);
    for (int i = 0; i < projected.buckets.size(); i++) {
        int total_count = 0;
        auto& range = projected.buckets[i].ranges[0];
        
        // 遍历原始直方图中的所有桶 
        for (auto& bucket : buckets) {
            // 如果当前桶的目标属性区间匹配，累加计数
            if (bucket.getLowValue(attr) == range.first && 
                bucket.getHighValue(attr) == range.second) {
                total_count += bucket.count;
            }
        }

        // 在新直方图中找到对应的桶并设置计数
        for (auto& new_bucket : projected.buckets) {
            if (new_bucket.getLowValue(attr) == range.first && 
                new_bucket.getHighValue(attr) == range.second) {
                    new_bucket.count = total_count;
                break;
            }
        }
    }
    return projected;
}

std::string Histogram::toString() {
    std::ostringstream oss;
    for (auto& bucket : this->buckets) {
        oss << "{ ";
        for (auto& attr : this->attrs) {
            oss << attr << "[" << bucket.getLowValue(attr) << "," << bucket.getHighValue(attr) << ") ";
        }
        oss << "} -> count=" << bucket.count << "\n";
    }
    return oss.str();
}

int Histogram::getAttrId(const std::string& attr) {
    auto it = std::find(attrs.begin(), attrs.end(), attr);
    return it != attrs.end() ? it - attrs.begin() : -1;
}


Index Histogram::generateIndex() {
    Index index;
    index.attrs = this->attrs;
    index.max_freqs = this->max_freqs;

    int current_pos = 0;
    for (const auto& bucket : this->buckets) {
        index.ranges.push_back(bucket.ranges);
        index.starts.push_back(current_pos);
        current_pos += bucket.count;
        index.ends.push_back(current_pos);
    }

    return index;
}
*/

std::string vec2Key(const std::vector<int>& values) {
    std::ostringstream oss;
    for (size_t i = 0; i < values.size(); i++){
        if (i > 0) {
            oss << ",";
        }
        oss << values[i];
    }
    return oss.str();
}

std::vector<int> key2Vec(const std::string& key) {
    std::vector<int> values;
    std::istringstream iss(key);
    std::string token;
    while (std::getline(iss, token, ',')) {
        values.push_back(std::stoi(token));
    }
    return values;
}

bool Histogram::isInRange(int value, const Range& range) {
    return value >= range.first && value <= range.second;
}

Histogram::Histogram(std::vector<std::string> &attrs, std::vector<std::vector<int>>& records, std::vector<Range>& ranges) : attrs(attrs), ranges(ranges) {
    // 初始化直方图，统计所有记录的频次
    counts.clear();
    original_count = 0;

    // 遍历每条记录
    for (const auto& record : records) {
        // 检查记录的维度是否匹配属性数量
        if (record.size() != attrs.size()) {
            std::cerr << "Error in construction of Histogram: Record dimension mismatch!" << std::endl;
            continue;
        }

        // 检查每个值是否在对应区间内
        bool valid = true;
        for (size_t i = 0; i < record.size(); i++) {
            if (!isInRange(record[i], ranges[i])) {
                valid = false;
                break;
            }
        }

        if (valid) {
            // 将记录转换为Key并增加计数
            std::string key = vec2Key(record);
            counts[key]++;
            original_count++;
        }
    }
}

Histogram::Histogram(const std::vector<std::string>& attrs, const std::vector<Range>& ranges, const std::unordered_map<std::string, int>& counts) 
: attrs(attrs), ranges(ranges), counts(counts) {
    original_count = 0;
    for (auto& entry : counts) {
        original_count += entry.second;
    }
}

int Histogram::getCount(const std::vector<int>& values) {
    if (values.size() != attrs.size()) {
        std::cerr << "Error in Histogram::getCount(): Query dimension mismatch!" << std::endl;
        return 0;
    }

    // 检查值是否在范围内
    for (size_t i = 0; i < values.size(); i++) {
        if (!isInRange(values[i], ranges[i])) {
            return 0;
        }
    }

    std::string key = vec2Key(values);
    auto it = counts.find(key);
    return (it != counts.end()) ? it->second : 0;
}

Histogram Histogram::addDPNoise(double epsilon, int type) {
    // 创建新的直方图副本
    std::unordered_map<std::string, int> noisy_counts = counts;
    
    // 计算敏感度 Delta_f
    // 对于直方图，敏感度通常为1（单条记录的改变最多影响一个桶的计数±1）
    double Delta_f = 1.0;
    
    // 为每个桶添加噪声
    for (auto& entry : noisy_counts) {
        int noise = 0;
        
        switch (type) {
            case 0: {
                // 随机噪声：生成正负随机的拉普拉斯噪声
                noise = genIntegerNoise(Delta_f, epsilon, LAPLACE_MECH);
                break;
            }
            case -1: {
                // 负向噪声：始终减去正整数噪声
                noise = -genIntegerNoise(Delta_f, epsilon, ONESIDED_LAPLACE_MECH);
                break;
            }
            case 1: {
                // 正向噪声：始终加上正整数噪声
                noise = genIntegerNoise(Delta_f, epsilon, ONESIDED_LAPLACE_MECH);
                break;
            }
            default:
                // 默认情况，不添加噪声
                noise = 0;
                break;
        }
        
        // 添加噪声到计数值
        entry.second += noise;
        
        // 确保计数值不为负数
        if (entry.second < 0) {
            entry.second = 0;
        }
    }

    auto noisy_histogram = Histogram(attrs, ranges, noisy_counts);
    noisy_histogram.setOriginalCount(this->original_count);
    
    // 返回新的带噪声的直方图
    return noisy_histogram;
}

Histogram Histogram::reduction(const std::vector<std::string>& sub_attrs) {
    // 检查sub_attrs是否真的是attrs的子集
    for (const auto& attr : sub_attrs) {
        if (std::find(attrs.begin(), attrs.end(), attr) == attrs.end()) {
            throw std::invalid_argument("Invalid Argument in Histogram::reduction(): Sub-attribute '" + attr + "' is not in the original attribute set");
        }
    }

    // 创建子属性索引映射，找出每个子属性在原始属性集中的位置
    std::vector<size_t> indices;
    std::vector<Range> sub_ranges;

    for (const auto& sub_attr : sub_attrs) {
        auto it = std::find(attrs.begin(), attrs.end(), sub_attr);
        size_t index = std::distance(attrs.begin(), it);
        indices.push_back(index);
        sub_ranges.push_back(ranges[index]);
    }

    // 用于新直方图的计数
    std::unordered_map<std::string, int> sub_counts;

    // 遍历原始直方图中所有的计数
    for (const auto& entry : counts) {
        std::vector<int> values = key2Vec(entry.first);
        // 提取子集属性对应的值
        std::vector<int> sub_values;
        for (size_t idx : indices) {
            sub_values.push_back(values[idx]);
        }
        // 构造新Key并累加计数
        std::string sub_key = vec2Key(sub_values);
        sub_counts[sub_key] += entry.second;
    }

    return Histogram(sub_attrs, sub_ranges, sub_counts);
}

int Histogram::getMaxFrequency() const {
    int max_freq = 0;
    for (const auto& entry : counts) {
        max_freq = std::max(max_freq, entry.second);
    }
    return max_freq;
}

int Histogram::getTotalCount() const {
    int count = 0;
    for (const auto& [key, val] : counts) {
        count += val;
    }
    return count;
}

std::string Histogram::toString() const {
    std::stringstream ss;
    
    // 输出取值范围
    ss << "Ranges: ";
    for (size_t i = 0; i < ranges.size(); ++i) {
        ss << attrs[i] << ": [" << ranges[i].first << ", " << ranges[i].second << "]";
        if (i < ranges.size() - 1) ss << ", ";
    }
    ss << std::endl;

    // 输出各数据点及其计数
    for (const auto& entry : counts) {  // 注意：从原来的示例代码看属性名可能是counters而非counts
        if (entry.second > 0) {
            ss << "(" << entry.first << ") -> Count: " << entry.second << std::endl;
        }
    }
    
    return ss.str();
}

HierarchicalHistogram::Node* HierarchicalHistogram::buildTree(int start, int end, int depth) {
    Node* node = new Node(start, end);
    if (depth == 0) return node;

    int mid = (start + end) / 2;
    node->left = buildTree(start, mid, depth - 1);
    node->right = buildTree(mid + 1, end, depth - 1);
    return node;
}

int HierarchicalHistogram::getTreeHeight(Node* node) const {
    if (!node) return 0;
    if (!node->left && !node->right) return 1;  // 叶子高度为1

    int leftHeight = node->left ? getTreeHeight(node->left) : 0;
    int rightHeight = node->right ? getTreeHeight(node->right) : 0;

    return 1 + std::max(leftHeight, rightHeight);
}

// 递归深拷贝树
HierarchicalHistogram::Node* HierarchicalHistogram::cloneTree(Node* originalNode) {
    if (!originalNode) return nullptr;
    
    Node* newNode = new Node(originalNode->range.first, originalNode->range.second);
    newNode->count = originalNode->count;
    
    if (originalNode->left) {
        newNode->left = cloneTree(originalNode->left);
    }
    
    if (originalNode->right) {
        newNode->right = cloneTree(originalNode->right);
    }
    
    return newNode;
}

int HierarchicalHistogram::computeCounts(Node* node) {
    if (!node->left && !node->right) return node->count;
    node->count = computeCounts(node->left) + computeCounts(node->right);
    return node->count;
}

HierarchicalHistogram::Node* HierarchicalHistogram::findLeaf(Node* node, int value) {
    if (!node->left && !node->right) {
        return (value >= node->range.first && value <= node->range.second) ? node : nullptr;
    }

    if (value <= node->left->range.second) {
        return findLeaf(node->left, value);
    }
    return findLeaf(node->right, value);
}

// 实现递归的范围查询辅助函数
int HierarchicalHistogram::rangeQueryHelper(Node *node, int queryStart, int queryEnd,
                                            std::vector<Node *> &selectedNodes) {
    // 基本情况：节点范围完全不在查询范围内
    if (node->range.second < queryStart || node->range.first > queryEnd) {
        return 0;
    }

    // 基本情况：节点范围完全包含在查询范围内
    if (node->range.first >= queryStart && node->range.second <= queryEnd) {
        selectedNodes.push_back(node);
        return node->count;
    }

    // 递归情况：查询范围与节点范围部分重叠，需要递归查询子节点
    int sum = 0;

    // 如果有左子节点，递归查询左子树
    if (node->left) {
        sum += rangeQueryHelper(node->left, queryStart, queryEnd, selectedNodes);
    }

    // 如果有右子节点，递归查询右子树
    if (node->right) {
        sum += rangeQueryHelper(node->right, queryStart, queryEnd, selectedNodes);
    }

    return sum;
}

// 递归地为树中每个节点添加噪声，并返回新的节点
HierarchicalHistogram::Node* HierarchicalHistogram::cloneNodeWithNoise(Node* originalNode, double epsilon_per_level, int type) {
    if (!originalNode) return nullptr;
    
    // 创建新节点
    Node* newNode = new Node(originalNode->range.first, originalNode->range.second);
    
    // 添加噪声
    if (type == -1) {
        // int noise = genIntegerNoise(1, epsilon_per_level, TRUNC_LAPLACE_MECH);
        int noise = genIntegerNoise(1, epsilon_per_level, ONESIDED_LAPLACE_MECH);
        newNode->count = originalNode->count - noise;
    } else if (type == 0) {
        int noise = genIntegerNoise(1, epsilon_per_level, LAPLACE_MECH);
        newNode->count = originalNode->count + noise;
    } else if (type == 1) {
        // int noise = genIntegerNoise(1, epsilon_per_level, TRUNC_LAPLACE_MECH);
        int noise = genIntegerNoise(1, epsilon_per_level, ONESIDED_LAPLACE_MECH);
        newNode->count = originalNode->count + noise;
    }
    
    // 确保计数非负
    if (newNode->count < 0) newNode->count = 0;
    
    // 递归处理子节点
    if (originalNode->left) {
        newNode->left = cloneNodeWithNoise(originalNode->left, epsilon_per_level, type);
    }
    
    if (originalNode->right) {
        newNode->right = cloneNodeWithNoise(originalNode->right, epsilon_per_level, type);
    }
    
    return newNode;
}

std::string HierarchicalHistogram::formatNode(Node* node) const {
    return "[" + std::to_string(node->range.first) + "-" 
        + std::to_string(node->range.second) + "] "
        + "(" + std::to_string(node->count) + ")";
}

std::string HierarchicalHistogram::nodeToString(Node* node, bool isLast, std::string prefix) const {
    if (!node) return "";
    
    std::string result;
    
    // 添加当前节点
    result += prefix;
    
    // 添加分支连接符
    if (isLast) {
        result += "└── ";
        prefix += "    "; // 为子节点添加4个空格
    } else {
        result += "├── ";
        prefix += "│   "; // 为子节点添加竖线和3个空格
    }
    
    // 添加节点信息
    result += formatNode(node) + "\n";
    
    // 判断是否有子节点
    if (node->left || node->right) {
        // 处理左子节点
        if (node->left) {
            bool leftIsLast = (node->right == nullptr);
            result += nodeToString(node->left, leftIsLast, prefix);
        }
        
        // 处理右子节点
        if (node->right) {
            result += nodeToString(node->right, true, prefix);
        }
    }
    
    return result;
}

HierarchicalHistogram::HierarchicalHistogram(const std::unordered_map<std::string, int>& counts, const Range& range) : original_range(range) {
    // 计算扩展范围
    int m = original_range.second - original_range.first + 1;
    int h = ceil(log2(m));
    int m_extended = 1 << h;
    int max_extended = original_range.first + m_extended - 1;
    original_count = 0;

    // 初始化完全二叉树结构
    root = buildTree(original_range.first, max_extended, h);

    // 填充叶子结点计数
    for (const auto& [key, val] : counts) {
        try {
            int num = std::stoi(key);
            if (Node* leaf = findLeaf(root, num)) {
                leaf->count += val;
            }
            // 累积计算total_count
            original_count += val;
        } catch (...) {

        }
    }

    // 计算聚合值
    computeCounts(root);
}

// 拷贝构造函数实现
HierarchicalHistogram::HierarchicalHistogram(const Node* sourceRoot, const Range& range, const int total_count) : original_range(range), original_count(total_count) {
    if (sourceRoot) {
        root = cloneTree(const_cast<Node*>(sourceRoot)); // 使用const_cast是因为cloneTree接受非const参数
    } else {
        root = nullptr;
    }
}

int HierarchicalHistogram::getMaxLeaf() {
    if (!root) return 0;
    
    int maxCount = 0;
    
    // 使用队列进行层序遍历，寻找所有叶子节点
    std::queue<Node*> q;
    q.push(root);
    
    while (!q.empty()) {
        Node* node = q.front();
        q.pop();
        
        // 检查是否为叶子节点（没有左右子节点）
        if (!node->left && !node->right) {
            maxCount = std::max(maxCount, node->count);
        } else {
            // 如果不是叶子节点，将子节点加入队列
            if (node->left) {
                q.push(node->left);
            }
            if (node->right) {
                q.push(node->right);
            }
        }
    }
    
    return maxCount;
}

// 实现公共范围查询方法
std::string HierarchicalHistogram::rangeQueryWithDetail(int queryStart, int queryEnd) {
    if (!root) return "Empty histogram";
    
    // 验证查询范围的有效性
    if (queryStart > queryEnd) {
        return "Invalid query range: start > end";
    }
    
    // 检查查询范围是否与直方图范围有重叠
    if (queryStart > original_range.second || queryEnd < original_range.first) {
        return "Query range [" + std::to_string(queryStart) + "-" + 
               std::to_string(queryEnd) + "] is outside histogram range [" + 
               std::to_string(original_range.first) + "-" + 
               std::to_string(original_range.second) + "]";
    }
    
    std::vector<Node*> selectedNodes;
    int totalCount = rangeQueryHelper(root, queryStart, queryEnd, selectedNodes);
    
    // 构建结果字符串
    std::string result = std::to_string(totalCount) + " {";
    for (size_t i = 0; i < selectedNodes.size(); ++i) {
        if (i > 0) result += ", ";
        result += formatNode(selectedNodes[i]);
    }
    result += "}";
    
    return result;
}

HierarchicalHistogram HierarchicalHistogram::addDPNoise(double epsilon, int type) {
    if (!root) return HierarchicalHistogram(nullptr, original_range, original_count);
    
    // 计算树的高度
    int height = getTreeHeight(root);
    
    // 计算每层的隐私预算
    double epsilon_per_level = epsilon / height;
    
    // 创建一个新的带噪声的树
    Node* noisyRoot = cloneNodeWithNoise(root, epsilon_per_level, type);
    
    // 使用新创建的根节点构造一个新的直方图对象
    HierarchicalHistogram noisyHistogram(noisyRoot, original_range, original_count);
    
    // 注意：我们已经不再需要重新计算聚合值，因为cloneNodeWithNoise函数会生成有噪声的节点，
    // 并且噪声是直接添加到每个节点上的。如果你需要一致性，可以调用：
    // noisyHistogram.computeCounts(noisyHistogram.root);
    
    return noisyHistogram;
}

int HierarchicalHistogram::rangeQuery(int queryStart, int queryEnd) {
    if (!root) return 0;
    
    // 验证查询范围的有效性
    if (queryStart > queryEnd) {
        return 0;
    }
    
    std::vector<Node*> selectedNodes;
    int totalCount = rangeQueryHelper(root, queryStart, queryEnd, selectedNodes);

    return totalCount;
}

std::string HierarchicalHistogram::toStringByTree() const {
    if (!root) return "Empty histogram";
    
    std::string result = formatNode(root) + "\n";
    
    if (root->left) {
        bool leftIsLast = (root->right == nullptr);
        result += nodeToString(root->left, leftIsLast, "");
    }
    
    if (root->right) {
        result += nodeToString(root->right, true, "");
    }
    
    return result;
}

std::string HierarchicalHistogram::toStringByLevel() const {
    if (!root) return "Empty histogram";
    
    std::string result;
    std::queue<std::pair<Node*, int>> q; // 队列存储节点和对应层级
    q.push({root, 0});
    
    int currentLevel = -1;
    std::string currentLevelStr;
    
    while (!q.empty()) {
        auto [node, level] = q.front();
        q.pop();
        
        // 如果进入新的层级，输出之前的层级信息
        if (level != currentLevel) {
            if (!currentLevelStr.empty()) {
                result += "h=" + std::to_string(currentLevel) + " => " + currentLevelStr + "\n";
            }
            currentLevel = level;
            currentLevelStr = formatNode(node);
        } else {
            // 同一层级，用 " | " 分隔节点信息
            currentLevelStr += " | " + formatNode(node);
        }
        
        // 将子节点加入队列
        if (node->left) {
            q.push({node->left, level + 1});
        }
        if (node->right) {
            q.push({node->right, level + 1});
        }
    }
    
    // 添加最后一层的信息
    if (!currentLevelStr.empty()) {
        result += "h=" + std::to_string(currentLevel) + " => " + currentLevelStr + "\n";
    }
    
    return result;
}

/*
Index::Index(const Histogram& histogram, const std::vector<int>& widths) : attrs(histogram.getAttrs()), widths(widths), ranges(histogram.getRanges()) {
    auto counts = histogram.getCounts();
    max_frequency = histogram.getMaxFrequency();

    // 计算每个属性的区间数
    std::vector<int> num_intervals(attrs.size());
    for (size_t i = 0; i < attrs.size(); i++) {
        num_intervals[i] = (ranges[i].second - ranges[i].first + widths[i] - 1) / widths[i];
    }

    // 枚举所有区间组合
    std::vector<std::pair<std::string, int>> all_intervals;
    std::vector<int> current_interval(attrs.size(), 0);

    // 递归生成所有区间组合
    std::function<void(size_t)> generate_intervals = [&](size_t idx) {
        if (idx == attrs.size()) {
            std::string key = vec2Key(current_interval);
            all_intervals.emplace_back(key, 0);
            return;
        }
        for (int i = 0; i < num_intervals[idx]; i++) {
            current_interval[idx] = i;
            generate_intervals(idx + 1);
        }
    };

    generate_intervals(0);

    // 计算每个区间的频次
    std::unordered_map<std::string, int> interval_counts;
    for (const auto& pair : counts) {
        std::vector<int> values = key2Vec(pair.first);
        std::vector<int> interval_ids(values.size());
        // 计算values[i]属于该属性的哪个区间
        for (size_t i = 0; i < values.size(); i++) {
            interval_ids[i] = (values[i] - ranges[i].first) / widths[i];
        }
        // 累加区间频次
        std::string interval_key = vec2Key(interval_ids);
        interval_counts[interval_key] += pair.second;
    }

    // 更新频次到all_intervals中
    for (auto& interval : all_intervals) {
        auto it = interval_counts.find(interval.first);
        if (it != interval_counts.end()) {
            interval.second = it->second;
        }
    }

    // 排序区间
    std::sort(all_intervals.begin(), all_intervals.end(), [this](const auto& a, const auto& b){
        return key2Vec(a.first) < key2Vec(b.first);
    });

    // 保存区间位置
    sorted_intervals = all_intervals;
    starts.resize(all_intervals.size());
    ends.resize(all_intervals.size());

    int pos = 0;
    for (size_t i = 0; i < sorted_intervals.size(); i++) {
        starts[i] = pos;
        pos += sorted_intervals[i].second;
        ends[i] = pos;
    }
}
*/

Index::Index(const Histogram& histogram, const std::vector<int>& widths, bool noise, double epsilon)
    : attrs(histogram.getAttrs()), widths(widths), ranges(histogram.getRanges()), epsilon(epsilon) {
    if (noise == true) {
        // 检查 histogram 是不是单属性
        if (histogram.getAttrs().size() != 1 || widths.size() != 1) {
            throw std::invalid_argument("Invalid Argument Error in Index(): histogram must be 1 dimension");
        }

        // 使用 histogram 生成分层直方图 hh+ 和 hh-
        HierarchicalHistogram hh(histogram.getCounts(), histogram.getRanges()[0]);
        HierarchicalHistogram hh_p = hh.addDPNoise(epsilon, 1);
        HierarchicalHistogram hh_n = hh.addDPNoise(epsilon, -1);

        // 格局 range 和 width 计算区间
        auto range = histogram.getRanges()[0];
        auto width = widths[0];

        for (int i = 0; i < ceil((range.second - range.first + 1) / double(width)); i++) { // 用 i 表示区间编号

            // 计算区间 i 的数值范围: [min, max]
            int min = range.first + i * width;
            int max = std::min(range.first + (i + 1) * width - 1, range.second);

            // 区间 i 的起始位置用 hh- 计算，相当于 range.first 到区间min之前的范围查询
            int start;
            if (min == range.first) { // 如果是第一个区间，那么start = 0
                start = 0;
            } else {
                start = hh_n.rangeQuery(range.first, min - 1);
            }

            // 区间 i 的终止位置用 hh+ 计算，相当于 range.first 到区间max之前的范围查询，确保索引值在合法范围内
            int end = std::min(hh_p.rangeQuery(range.first, max), histogram.getTotalCount());

            // 将计算结果存入 Index 中
            starts.push_back(start);
            ends.push_back(end);
            sorted_intervals.push_back({std::to_string(i), end - start});
        }
    } else {
        auto counts = histogram.getCounts();
    
        // 计算每个属性的区间数
        std::vector<int> num_intervals(attrs.size());
        for (size_t i = 0; i < attrs.size(); i++) {
            num_intervals[i] = std::ceil((ranges[i].second - ranges[i].first + 1) / double(widths[i]));
        }
    
        // 枚举所有区间组合
        std::vector<std::pair<std::string, int>> all_intervals;
        std::vector<int> current_interval(attrs.size(), 0);
    
        // 递归生成所有区间组合
        std::function<void(size_t)> generate_intervals = [&](size_t idx) {
            if (idx == attrs.size()) {
                std::string key = vec2Key(current_interval);
                all_intervals.emplace_back(key, 0);
                return;
            }
            for (int i = 0; i < num_intervals[idx]; i++) {
                current_interval[idx] = i;
                generate_intervals(idx + 1);
            }
        };
    
        generate_intervals(0);
    
        // 计算每个区间的频次
        std::unordered_map<std::string, int> interval_counts;
        for (const auto& pair : counts) {
            std::vector<int> values = key2Vec(pair.first);
            std::vector<int> interval_ids(values.size());
            // 计算values[i]属于该属性的哪个区间
            for (size_t i = 0; i < values.size(); i++) {
                interval_ids[i] = (values[i] - ranges[i].first) / widths[i];
            }
            // 累加区间频次
            std::string interval_key = vec2Key(interval_ids);
            interval_counts[interval_key] += pair.second;
        }
    
        // 更新频次到all_intervals中
        for (auto& interval : all_intervals) {
            auto it = interval_counts.find(interval.first);
            if (it != interval_counts.end()) {
                interval.second = it->second;
            }
        }
    
        // 排序区间
        std::sort(all_intervals.begin(), all_intervals.end(), [this](const auto& a, const auto& b){
            return key2Vec(a.first) < key2Vec(b.first);
        });
    
        // 保存区间位置
        sorted_intervals = all_intervals;
        starts.resize(all_intervals.size());
        ends.resize(all_intervals.size());
    
        int pos = 0;
        for (size_t i = 0; i < sorted_intervals.size(); i++) {
            starts[i] = pos;
            pos += sorted_intervals[i].second;
            ends[i] = pos;
        } 
    }
}

Index::Index(Histogram& h_p, Histogram& h_n) 
    : attrs(h_p.getAttrs()), ranges(h_p.getRanges()) {
    // 检查 histogram 是不是单属性
    if (h_p.getAttrs().size() != 1) {
        throw std::invalid_argument("Invalid Argument Error in Index(): histogram must be 1 dimension");
    }

    // 格局 range 计算区间
    auto range = ranges[0];

    // 默认划分为 10 个区间；可根据实际需求调整
    int total_range = range.second - range.first + 1;
    int num_intervals = 10;
    int width = static_cast<int>(std::ceil(double(total_range) / num_intervals));
    widths.push_back(width);

    for (int i = 0; i < ceil((range.second - range.first + 1) / double(width)); i++) { // 用 i 表示区间编号
        // 计算区间 i 的数值范围: [min, max]
        int min = range.first + i * width;
        int max = std::min(range.first + (i + 1) * width - 1, range.second);

        // 区间 i 的起始位置用 h- 累积和计算
        int start = 0;
        for (int j = range.first; j < min; j++) {
            start += h_n.getCount({j});
        }

        // 区间 i 的终止位置用 h+ 累积和计算
        int end = 0;
        for (int j = range.first; j <= max; j++) {
            end += h_p.getCount({j});
        }

        // 将结果存入 Index 中
        starts.push_back(std::max(0, start));
        ends.push_back(std::min(end, h_p.getOriginalCount()));
        sorted_intervals.push_back({std::to_string(i), end - start});
    }
}

Index::Index(std::string attr, HierarchicalHistogram& hh_p, HierarchicalHistogram& hh_n) {
    // 假定直方图均为一维，因此：
    // 从 hh_p 中获取原始取值区间，该接口需要在 HierarchicalHistogram 中实现
    Range range = hh_p.getOriginalRange();  // 例如实现为：return original_range;
    
    // 设置属性名称（这里只用一个属性，可以根据需要自定义）
    attrs.push_back(attr);
    ranges.push_back(range);
    
    // 默认划分为 10 个区间；可根据实际需求调整
    int total_range = range.second - range.first + 1;
    int num_intervals = 10;
    int width = static_cast<int>(std::ceil(double(total_range) / num_intervals));
    widths.push_back(width);

    // 根据区间宽度划分区间个数，注意：可能最后一个区间宽度不足
    int n_intervals = static_cast<int>(std::ceil(double(total_range) / width));
    for (int i = 0; i < n_intervals; i++) {
        // 区间 i 的左右边界
        int min_val = range.first + i * width;
        int max_val = std::min(range.first + (i + 1) * width - 1, range.second);
        
        // 使用 hh_n（负向噪声直方图）计算本区间前的累积计数
        int start_index = (min_val == range.first) ? 0 : hh_n.rangeQuery(range.first, min_val - 1);
        // 使用 hh_p（正向噪声直方图）计算本区间的累计计数，确保不超过 totalCount
        int end_index = std::min(hh_p.rangeQuery(range.first, max_val), hh_p.getOriginalCount());
        
        starts.push_back(start_index);
        ends.push_back(end_index);
        // 存储区间编号（这里用字符串表示区间 id）以及该区间的计数差（作为频次）
        sorted_intervals.push_back({ std::to_string(i), end_index - start_index });
    }
}

Range Index::findInterval(const std::vector<int>& values) const {
    std::vector<int> interval_ids(values.size());
    for (size_t i = 0; i < values.size(); i++) {
        // interval_ids[i] = (values.size() - ranges[i].first) / widths[i];
        interval_ids[i] = (values[i] - ranges[i].first) / widths[i];
    }
    std::string key = vec2Key(interval_ids);

    // 二分查找区间位置
    int l = 0, r = starts.size() - 1;
    while (l <= r) {
        int m = (l + r) / 2;
        auto cur_ids = key2Vec(sorted_intervals[m].first);
        if (cur_ids == interval_ids) {
            return {starts[m], ends[m]};
        } else if (cur_ids < interval_ids) {
            l = m + 1;
        } else {
            r = m - 1;
        }
    }
    return {-1, -1};    // 未找到区间，返回无效位置
}

std::vector<Range> Index::getRanges(int idx) const {
    std::vector<Range> interval_ranges;
    for (int i = 0; i < attrs.size(); i++) {
        interval_ranges.push_back({ranges[i].first + i * widths[i], std::min(ranges[i].first + (i + 1) * widths[i], ranges[i].second)});
    }
    return interval_ranges;
}

std::string Index::toString() const {
    std::ostringstream oss;
    oss << "Attributes: ";
    for (const auto& attr : attrs)
        oss << attr << " ";
    oss << "\nWidths: ";
    for (const auto& w : widths)
        oss << w << " ";

    for (size_t i = 0; i < sorted_intervals.size(); ++i) {
        std::vector<int> interval_ids = key2Vec(sorted_intervals[i].first);

        // Compute interval start values (xi)
        oss << "(";
        for (size_t j = 0; j < interval_ids.size(); ++j) {
            int start_value = ranges[j].first + interval_ids[j] * widths[j];
            oss << start_value;
            if (j < interval_ids.size() - 1)
                oss << ",";
        }

        oss << ") ~ (";

        // Compute interval end values (yi)
        for (size_t j = 0; j < interval_ids.size(); ++j) {
            int end_value = ranges[j].first + (interval_ids[j] + 1) * widths[j];
            oss << end_value;
            if (j < interval_ids.size() - 1)
                oss << ",";
        }
        oss << ")";

        oss << " : [" << starts[i] << ", " << ends[i] << ") -> Count: " << sorted_intervals[i].second << "\n";
    }
    return oss.str();
}