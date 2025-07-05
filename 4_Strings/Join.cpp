#include "JoinUtils.hpp"
#include <vector>
#include <omp.h>
using namespace std;

class TrieNode {
public:
    bool endOfWord;
    TrieNode* children[27]{};
    vector<const CastRelation*> cast;  // Const-correctness
    TrieNode() : endOfWord(false) {
        for(auto & child : children) {
            child = nullptr;
        }
    }
};

class Trie {
private:
    TrieNode* root;

public:
    Trie() { root = new TrieNode(); }
    ~Trie() { freeTrie(root); }

    void insert(const CastRelation* cast) {  // Const-correctness
        TrieNode* node = root;
        for(auto c : cast->note) {
            int index = tolower(static_cast<unsigned char>(c)) - 'a';
            if (index < 0 || index > 25) index = 26;

            if(!node->children[index]) {
                node->children[index] = new TrieNode();
            }
            node = node->children[index];
        }
        node->endOfWord = true;
        node->cast.emplace_back(cast);
    }
        TrieNode* node = root;
        for (auto c : title->title) {
            int index = tolower(static_cast<unsigned char>(c)) - 'a';
            if (index < 0 || index > 25) index = 26;

            if (node->endOfWord) {

                result.insert(result.end(), node->cast.begin(), node->cast.end());
            }

            if (!node->children[index]) return;
            node = node->children[index];
        }
        if (node->endOfWord) {
            result.insert(result.end(), node->cast.begin(), node->cast.end());
        }
    }

    static void freeTrie(TrieNode* node) {
        for (auto & child : node->children) {
            if (child) {
                freeTrie(child);


    static void  freeTrie(TrieNode* node) {
        for (auto & i : node->children) {
            if (i != nullptr) {
                freeTrie(i);
            }
        }
        delete node;
    }
};

std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation,
                                       const std::vector<TitleRelation>& titleRelation,
                                       int numThreads) {
    omp_set_num_threads(numThreads);
    std::vector<ResultRelation> resultTuples;

    Trie trie;  // Stack-Allokation statt Heap!

    // Trie aufbauen (single-threaded)
    for (const auto& cast : castRelation) {
        trie.insert(const_cast<CastRelation*>(&cast));
    }

    // Thread-lokale Ergebnisse
    std::vector<std::vector<ResultRelation>> threadLocalResults(numThreads);

    #pragma omp parallel num_threads(numThreads)
    {
        const int tid = omp_get_thread_num();
        auto& localResults = threadLocalResults[tid];

        // Thread-lokaler Puffer für Casts
        std::vector<const CastRelation*> casts;
        casts.reserve(20);  // Höhere Reserve für mehr Treffer

        #pragma omp for schedule(static)
        for (size_t i = 0; i < titleRelation.size(); i++) {
            const auto& title = titleRelation[i];  // Referenz verwenden
            casts.clear();
            trie.find(&title, casts);

            for (auto element : casts) {
                localResults.emplace_back(createResultTuple(*element, title));
            }
        }
    }


    // Ergebnisse zusammenführen (mit move)
    size_t totalSize = 0;
    for (const auto& vec : threadLocalResults) {
        totalSize += vec.size();
    }
    resultTuples.reserve(totalSize);

    for (auto& localVec : threadLocalResults) {
        resultTuples.insert(resultTuples.end(),
                          std::make_move_iterator(localVec.begin()),
                          std::make_move_iterator(localVec.end()));
    }

    return resultTuples;
}