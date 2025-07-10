#include "Test/Test.h"

int main() {

    // Test test(limit_test);
    // Test test(graph_submatch_test);
    // Test test(join_plan_test);
    // Test test(basic_test);
    // Test test(table_test);
    // Test test(table_index_test);
    // Test test(table_bucketjoin_test);
    // Test test(get_symmetries_test);
    // Test test(compute_AGMBound_test);
    // Test test(checkMatch_protocol_test);
    // Test test(star_decomposition_test);
    // Test test(mix_upper_bound_test);
    // Test test(hash_join_test);

    // Test test(example_test);
    // Test test(GRQC_test);
    // Test test(Euroroads_test);
    // Test test(end_to_end_test, "data_example_SWW12", "query_example_SWW12", true, true);
    Test test(end_to_end_test, "data_Dolphins", "query_K4", true, true, true, true, 0.2, 1.8);

    test.run();
    test.printMetrics();

    // std::vector<double> epsilons = {0.1, 0.3, 0.5, 1.0, 2.0, 5.0, 10.0};

    // for (int i = 0; i < epsilons.size(); i++) {
    //     for (int j = 0; j < epsilons.size(); j++) {
    //         sleep(5);
    //         std::cout << "-----------------------------------------" << std::endl;
    //         std::cout << "e_idx = " << epsilons[i] << ", e_mf = " << epsilons[j] << std::endl;
    //         Test test(end_to_end_test, "data_Dolphins", "query_K4", true, true, true, epsilons[i], epsilons[j]);
    //         test.run();
    //         test.printMetrics();
    //     }
    // }
    
    
    return 0;
}