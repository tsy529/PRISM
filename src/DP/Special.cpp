#include "Special.h"

double genNoise(double Delta_f, double epsilon, NoiseType mechanism) {
    static std::mt19937 rng(std::random_device{}());
    double noise = 0.0;
    if (mechanism == LAPLACE_MECH) {
        std::uniform_real_distribution<double> uniform(0.0, 1.0);
        double u = uniform(rng);
        double b = Delta_f / epsilon;
        if (u < 0.5) {
            noise = b * std::log(2.0 * u);
        } else {
            noise = -b * std::log(2.0 * (1.0 - u));
        }
    } else if (mechanism == TRUNC_LAPLACE_MECH) {
        std::uniform_real_distribution<double> uniform(0.0, 1.0);
        double u = uniform(rng);
        double b = Delta_f / epsilon;
        if (u < 0.5) {
            noise = b * std::log(2.0 * u);
        } else {
            noise = -b * std::log(2.0 * (1.0 - u));
        }
        noise = std::max(0.0, noise);
    } else if (mechanism == ONESIDED_LAPLACE_MECH) {
        // 计算拉普拉斯分布的尺度参数 b（通常 b = Δ_f/ε）
        double b = Delta_f / epsilon;
        // 根据要求设定平移量 μ，使得 P{noise < 0} = δ
        // 根据对称拉普拉斯分布的性质：
        //      P{X < -μ} = 1/2 · exp(-μ/b)
        // 令 1/2 · exp(-μ/b) = δ 可得：
        double mu = -b * std::log(2 * delta);

        // 使用逆变换采样法生成对称拉普拉斯分布的样本 X ~ Laplace(0, b)
        std::uniform_real_distribution<double> uniform(0.0, 1.0);
        double u = uniform(rng);

        double x; // 对称拉普拉斯分布采样结果
        if (u < 0.5) {
            x = b * std::log(2 * u);
        } else {
            x = -b * std::log(2 * (1 - u));
        }

        // 将噪声右移 μ，并采用 max(0, ·) 得到仅正向的噪声
        noise = std::max(0.0, x + mu);   // 对应的概率正好为 δ
    }
    return noise;
}

int genIntegerNoise(double Delta_f, double epsilon, NoiseType mechanism) {
    return static_cast<int>(std::round(genNoise(Delta_f, epsilon, mechanism)));
}