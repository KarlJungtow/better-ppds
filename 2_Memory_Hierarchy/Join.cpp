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

// Optimized parallel hash join with cache-friendly chunking
vector<ResultRelation> performJoin(const vector<CastRelation>& castRelation,
                                   const vector<TitleRelation>& titleRelation,
                                   int numThreads) {
    // Handle empty input cases
    if (castRelation.empty() || titleRelation.empty()) {
        return {};
    }

    // Determine smaller relation for building hash table
    const bool castIsSmaller = castRelation.size() <= titleRelation.size();
    const vector<CastRelation>& smallerCast = castIsSmaller ? castRelation : vector<CastRelation>();
    const vector<TitleRelation>& smallerTitle = !castIsSmaller ? titleRelation : vector<TitleRelation>();

    // Calculate chunk size based on cache size (256KB)
    const size_t cacheSizeBytes = 256 * 1024;
    const size_t elementSize = castIsSmaller ? sizeof(CastRelation) : sizeof(TitleRelation);
    const size_t chunkSize = max(static_cast<size_t>(1), cacheSizeBytes / elementSize);

    // Precompute the larger relation size for partitioning
    const size_t largerSize = castIsSmaller ? titleRelation.size() : castRelation.size();

    // Build hash table for the smaller relation
    unordered_map<int, vector<const CastRelation*>> castMap;
    unordered_map<int, vector<const TitleRelation*>> titleMap;

    if (castIsSmaller) {
        for (const auto& cr : smallerCast) {
            castMap[cr.movieId].push_back(&cr);
        }
    } else {
        for (const auto& tr : smallerTitle) {
            titleMap[tr.titleId].push_back(&tr);
        }
    }

    // Pre-allocate thread results
    vector<vector<ResultRelation>> threadResults(numThreads);
    for (auto& vec : threadResults) {
        vec.reserve(largerSize / numThreads * 2);  // Heuristic reserve
    }

    // Calculate chunks for cache-friendly processing
    const size_t numChunks = (largerSize + chunkSize - 1) / chunkSize;

    // Parallel probe phase with cache-friendly chunking
    #pragma omp parallel num_threads(numThreads)
    {
        const int tid = omp_get_thread_num();

        // Process assigned chunks
        for (size_t chunk = tid; chunk < numChunks; chunk += numThreads) {
            const size_t start = chunk * chunkSize;
            const size_t end = min(start + chunkSize, largerSize);

            if (castIsSmaller) {
                for (size_t i = start; i < end; i++) {
                    const TitleRelation& tr = titleRelation[i];
                    auto it = castMap.find(tr.titleId);
                    if (it != castMap.end()) {
                        for (const auto& crPtr : it->second) {
                            threadResults[tid].push_back(createResultTuple(*crPtr, tr));
                        }
                    }
                }
            } else {
                for (size_t i = start; i < end; i++) {
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