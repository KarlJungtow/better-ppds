#include "JoinUtils.hpp"
#include <vector>
#include <memory>
#include <cctype>
#include <string_view>
#include <string>
#include <omp.h>  // Include OpenMP header
using namespace std;

// Constants for Trie configuration (unchanged)
static constexpr int ALPHABET_SIZE = 26;
static constexpr int OTHER_INDEX = ALPHABET_SIZE;
static constexpr int TOTAL_CHILDREN = ALPHABET_SIZE + 1;

class Trie {
private:
    struct TrieNode {
        bool endOfWord = false;
        vector<const CastRelation*> cast;
        unique_ptr<TrieNode> children[TOTAL_CHILDREN];
    };

    unique_ptr<TrieNode> root;

    static int charToIndex (char c){
        c = tolower(c);
        if (c >= 'a' && c <= 'z') {
            return c - 'a';
        }
        return OTHER_INDEX;
    }

public:
    Trie() : root(make_unique<TrieNode>()) {}

    void insert(const CastRelation* cast) const {
        TrieNode* node = root.get();
        string_view note(cast->note);

        for (char c : note) {
            int index = charToIndex(c);
            if (!node->children[index]) {
                node->children[index] = make_unique<TrieNode>();
            }
            node = node->children[index].get();
        }

        node->endOfWord = true;
        node->cast.push_back(cast);
    }

    void findPrefixMatches(const string& prefix, vector<const CastRelation*>& results) const {
        TrieNode* node = root.get();

        for (char c : prefix) {
            if (node->endOfWord) {
                results.insert(results.end(), node->cast.begin(), node->cast.end());
            }

            int index = charToIndex(c);
            if (!node->children[index]) return;
            node = node->children[index].get();
        }

        if (node->endOfWord) {
            results.insert(results.end(), node->cast.begin(), node->cast.end());
        }
    }
};
//-------------------------------------------------------------------------------------------------------------------------

 vector<ResultRelation> performJoin(const vector<CastRelation>& castRelation,
                                    const vector<TitleRelation>& titleRelation,
                                    int numThreads) {
     // Single-threaded Trie construction
     Trie trie;
     for (const auto& cast : castRelation) {
         trie.insert(&cast);
     }

     vector<ResultRelation> resultTuples;
     resultTuples.reserve(titleRelation.size() * 2);

     // Parallel section for title processing
#pragma omp parallel num_threads(numThreads)
     {
         // Thread-local storage for matches and results
         vector<ResultRelation> localResults;
         localResults.reserve(titleRelation.size() * 2 / omp_get_num_threads());

#pragma omp for schedule(static)
         for (const auto & title : titleRelation) {
             vector<const CastRelation*> prefixMatches;
             prefixMatches.reserve(10);
             trie.findPrefixMatches(title.title, prefixMatches);

             for (const auto cast : prefixMatches) {
                 localResults.emplace_back(createResultTuple(*cast, title));
             }
         }

         // Combine local results (critical section)
#pragma omp critical
         {
             resultTuples.insert(resultTuples.end(),make_move_iterator(localResults.begin()), make_move_iterator(localResults.end()));
         }

         return resultTuples;
     }
 }