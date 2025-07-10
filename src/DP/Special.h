#pragma once

#include <iostream>
#include <vector>
#include <set>
#include <algorithm>
#include <cmath>
#include <random>
#include <sstream>

using NoiseType = u_int;

static const NoiseType LAPLACE_MECH = 1;
static const NoiseType TRUNC_LAPLACE_MECH = 2;
static const NoiseType ONESIDED_LAPLACE_MECH = 3;
static constexpr double epsilon = 1.5;
static constexpr double delta = 0.001;

double genNoise(double Delta_f, double epsilon, NoiseType mechanism);

int genIntegerNoise(double Delta_f, double epsilon, NoiseType mechanism);