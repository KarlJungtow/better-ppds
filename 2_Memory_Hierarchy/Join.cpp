/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include "TimerUtil.hpp"
#include "JoinUtils.hpp"
#include <unordered_map>
#include <iostream>
#include <gtest/gtest.h>
#include <omp.h>

std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {

    // IMPORTANT: You can assume for this benchmark that the join keys are sorted in both relations.

    omp_set_num_threads(numThreads);

    std::vector<ResultRelation> resultTuples;



    int pointer_cast = 0;

    int pointer_title = 0;

    int old_position = 0;



    while(pointer_cast < castRelation.size() && pointer_title < titleRelation.size()) {

        if(castRelation[pointer_cast].movieId < titleRelation[pointer_title].titleId) {

            pointer_cast++;

        } else if(castRelation[pointer_cast].movieId > titleRelation[pointer_title].titleId) {

            pointer_title++;

        } else {

            old_position = pointer_cast;

            while(pointer_cast < castRelation.size() && castRelation[pointer_cast].movieId == titleRelation[pointer_title].titleId) {

                resultTuples.push_back(createResultTuple(castRelation[pointer_cast], titleRelation[pointer_title]));

                pointer_cast++;

            }

            pointer_cast = old_position;

            pointer_title++;

        }

    }



    return resultTuples;

}