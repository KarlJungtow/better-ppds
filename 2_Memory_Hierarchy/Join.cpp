#include "JoinUtils.hpp"
#include <gtest/gtest.h>
#include <omp.h>
#include <vector>
#include <iostream>
#include <string>
#include <chrono>
#include <algorithm>  // Added for std::min/max
using namespace std;

uint_fast32_t splitTitle(const vector<TitleRelation>& titleRelation, int index_of_cutoff) {
    // Added negative index check
    if (index_of_cutoff < 0 || index_of_cutoff >= static_cast<int>(titleRelation.size()))
        return 0;

    int current_id = titleRelation[index_of_cutoff].titleId;
    int current_index = index_of_cutoff;

    // Fixed: Added bounds check for current_index - 1
    if (current_index + 1 < static_cast<int>(titleRelation.size()) &&
        titleRelation[current_index + 1].titleId != current_id) {
        while (current_index >= 1 && titleRelation[current_index - 1].titleId == current_id) {
            current_index--;
        }
    }

    return current_index;
}

uint_fast32_t backTitle(const vector<TitleRelation>& titleRelation, int movieId, int index_of_cutoff) {
    // Added bounds check
    if (titleRelation.empty()) return 0;

    int current_index = min(index_of_cutoff, static_cast<int>(titleRelation.size()) - 1);
    // Fixed: Prevent underflow with >= 1 check
    while (current_index >= 1 && titleRelation[current_index].titleId > movieId) {
        current_index--;
    }
    return current_index;
}

uint_fast32_t splitCast(const vector<CastRelation>& castRelation, int index_of_cutoff) {
    // Added negative index check
    if (index_of_cutoff < 0 || index_of_cutoff >= static_cast<int>(castRelation.size()))
        return 0;

    int current_id = castRelation[index_of_cutoff].movieId;
    int current_index = index_of_cutoff;

    // Fixed: Added bounds check for current_index - 1
    if (current_index + 1 < static_cast<int>(castRelation.size()) &&
        castRelation[current_index + 1].movieId != current_id) {
        while (current_index >= 1 && castRelation[current_index - 1].movieId == current_id) {
            current_index--;
        }
    }

    return current_index;
}

uint_fast32_t backCast(const vector<CastRelation>& castRelation, int titleId, int index_of_cutoff) {
    // Added bounds check
    if (castRelation.empty()) return 0;

    int current_index = min(index_of_cutoff, static_cast<int>(castRelation.size()) - 1);
    // Fixed: Prevent underflow with >= 1 check
    while (current_index >= 1 && castRelation[current_index].movieId > titleId) {
        current_index--;
    }
    return current_index;
}

vector<uint_fast32_t> splitRelations(const vector<CastRelation>& castRelation,
                                     const vector<TitleRelation>& titleRelation,
                                     int index_of_cutoff) {
    // Enhanced bounds checking (negative and both vectors)
    if (castRelation.empty() || titleRelation.empty() ||
        index_of_cutoff < 0 ||
        index_of_cutoff >= static_cast<int>(castRelation.size()) ||
        index_of_cutoff >= static_cast<int>(titleRelation.size())) {
        return {0, 0};
    }

    int title_id = titleRelation[index_of_cutoff].titleId;
    int cast_id = castRelation[index_of_cutoff].movieId;

    if (title_id > cast_id) {
        int title_index = splitTitle(titleRelation, index_of_cutoff);
        // Fixed: Ensure we don't access out-of-bounds
        int cast_index = backCast(castRelation, titleRelation[title_index].titleId, index_of_cutoff);
        return {static_cast<uint_fast32_t>(title_index), static_cast<uint_fast32_t>(cast_index)};
    } else {
        int cast_index = splitCast(castRelation, index_of_cutoff);
        // Fixed: Ensure we don't access out-of-bounds
        int title_index = backTitle(titleRelation, castRelation[cast_index].movieId, index_of_cutoff);
        return {static_cast<uint_fast32_t>(title_index), static_cast<uint_fast32_t>(cast_index)};
    }
}

vector<ResultRelation> performJoinThread(const vector<CastRelation>& castRelation,
                                         const vector<TitleRelation>& titleRelation) {
    vector<ResultRelation> resultTuples;
    size_t pointer_cast = 0;
    size_t pointer_title = 0;
    size_t old_position = 0;

    // Fixed: Use size_t to prevent negative indices
    while (pointer_cast < castRelation.size() && pointer_title < titleRelation.size()) {
        if (castRelation[pointer_cast].movieId < titleRelation[pointer_title].titleId) {
            pointer_cast++;
        } else if (castRelation[pointer_cast].movieId > titleRelation[pointer_title].titleId) {
            pointer_title++;
        } else {
            old_position = pointer_cast;
            while (pointer_cast < castRelation.size() &&
                   castRelation[pointer_cast].movieId == titleRelation[pointer_title].titleId) {
                resultTuples.push_back(createResultTuple(castRelation[pointer_cast],
                                                        titleRelation[pointer_title]));
                pointer_cast++;
            }
            pointer_cast = old_position;
            pointer_title++;
        }
    }

    return resultTuples;
}

vector<ResultRelation> performJoin(const vector<CastRelation>& castRelation,
                                   const vector<TitleRelation>& titleRelation,
                                   int numThreads) {
    const int half_cache_size_bytes = 256 * 1024;
    const size_t index_of_cutoff = half_cache_size_bytes / sizeof(CastRelation);

    if (castRelation.empty() || titleRelation.empty()) {
        return {};
    }

    vector<vector<CastRelation>> castSlices;
    vector<vector<TitleRelation>> titleSlices;
    vector<ResultRelation> resultRelation;
    size_t offset = 0;

    while (offset < castRelation.size() && offset < titleRelation.size()) {
        // Calculate next cutoff point
        size_t next_cutoff_index = offset + index_of_cutoff;

        // Adjust if beyond vector sizes
        if (next_cutoff_index >= castRelation.size() ||
            next_cutoff_index >= titleRelation.size()) {
            next_cutoff_index = min(castRelation.size(), titleRelation.size()) - 1;
        }

        vector<uint_fast32_t> splitIndices = splitRelations(castRelation, titleRelation, next_cutoff_index);
        size_t title_cutoff = splitIndices[0];
        size_t cast_cutoff = splitIndices[1];

        // Clamp cutoffs to valid ranges
        title_cutoff = min(title_cutoff, titleRelation.size());
        cast_cutoff = min(cast_cutoff, castRelation.size());

        // Ensure we make progress
        if (title_cutoff <= offset || cast_cutoff <= offset) {
            break;
        }

        // Create slices
        castSlices.emplace_back(castRelation.begin() + offset,
                               castRelation.begin() + cast_cutoff);
        titleSlices.emplace_back(titleRelation.begin() + offset,
                                titleRelation.begin() + title_cutoff);

        // Move to next chunk (use max to prevent overlapping)
        offset = max(title_cutoff, cast_cutoff);
    }

    // Process remaining elements
    if (offset < castRelation.size() && offset < titleRelation.size()) {
        castSlices.emplace_back(castRelation.begin() + offset, castRelation.end());
        titleSlices.emplace_back(titleRelation.begin() + offset, titleRelation.end());
    }

    vector<vector<ResultRelation>> thread_results(castSlices.size());

    #pragma omp parallel for schedule(dynamic) num_threads(numThreads)
    for (int i = 0; i < static_cast<int>(castSlices.size()); ++i) {
        thread_results[i] = performJoinThread(castSlices[i], titleSlices[i]);
    }

    // Combine results
    size_t totalSize = 0;
    for (const auto& res : thread_results) {
        totalSize += res.size();
    }
    resultRelation.reserve(totalSize);
    for (const auto& vec : thread_results) {
        resultRelation.insert(resultRelation.end(), vec.begin(), vec.end());
    }

    return resultRelation;
}