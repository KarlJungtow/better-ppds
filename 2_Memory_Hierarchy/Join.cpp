#include "JoinUtils.hpp"
#include <gtest/gtest.h>
#include <omp.h>
#include <vector>
#include <iostream>
#include <string>
#include <chrono>
#include <algorithm>
#include <unordered_map>
#include <cmath>
using namespace std;

// Optimized hash join implementation
vector<ResultRelation> performJoin(const vector<CastRelation>& castRelation,
                                   const vector<TitleRelation>& titleRelation,
                                   int numThreads) {
    // Handle empty input cases
    if (castRelation.empty() || titleRelation.empty()) {
        return {};
    }

    // Determine smaller relation for building hash table
    const bool castIsSmaller = castRelation.size() <= titleRelation.size();
    const size_t largerSize = castIsSmaller ? titleRelation.size() : castRelation.size();

    // Calculate optimal chunk size (256KB cache-friendly)


    // Build hash table for smaller relation
    unordered_map<int, vector<const CastRelation*>> castMap;
    unordered_map<int, vector<const TitleRelation*>> titleMap;

    if (castIsSmaller) {
        for (const auto& cr : castRelation) {
            castMap[cr.movieId].push_back(&cr);
        }
    } else {
        for (const auto& tr : titleRelation) {
            titleMap[tr.titleId].push_back(&tr);
        }
    }

    // Pre-allocate thread results
    vector<vector<ResultRelation>> threadResults(numThreads);
    for (auto& vec : threadResults) {
        vec.reserve(largerSize / numThreads * 2);  // Heuristic reserve
    }

    // Parallel probe phase
    #pragma omp parallel num_threads(numThreads)
    {
        const int tid = omp_get_thread_num();
        const size_t startIdx = (largerSize * tid) / numThreads;
        const size_t endIdx = (largerSize * (tid + 1)) / numThreads;

        if (castIsSmaller) {
            for (size_t i = startIdx; i < endIdx; i++) {
                const TitleRelation& tr = titleRelation[i];
                auto it = castMap.find(tr.titleId);
                if (it != castMap.end()) {
                    for (const auto& crPtr : it->second) {
                        threadResults[tid].push_back(createResultTuple(*crPtr, tr));
                    }
                }
            }
        } else {
            for (size_t i = startIdx; i < endIdx; i++) {
                const CastRelation& cr = castRelation[i];
                auto it = titleMap.find(cr.movieId);
                if (it != titleMap.end()) {
                    for (const auto& trPtr : it->second) {
                        threadResults[tid].push_back(createResultTuple(cr, *trPtr));
                    }
                }
            }
        }
    }

    // Combine results
    size_t totalResults = 0;
    for (const auto& vec : threadResults) {
        totalResults += vec.size();
    }

    vector<ResultRelation> finalResult;
    finalResult.reserve(totalResults);
    for (auto& vec : threadResults) {
        finalResult.insert(finalResult.end(),
                          make_move_iterator(vec.begin()),
                          make_move_iterator(vec.end()));
    }

    return finalResult;
}