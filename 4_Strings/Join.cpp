#include "JoinUtils.hpp"

#include <vector>
#include <omp.h>

using namespace std;

class TrieNode {
public:
    bool endOfWord;
    TrieNode* children[27]{};
    vector<CastRelation*> cast;

    TrieNode() : endOfWord(false) {
        for (auto& child : children) {
            child = nullptr;
        }
    }
};

class Trie {
private:
    TrieNode* root;

public:
    Trie() {
        root = new TrieNode();
    }

    ~Trie() {
        freeTrie(root);
    }

    void insert(CastRelation* cast) const {
        TrieNode* node = root;
        for (auto c : cast->note) {
            int index = tolower(static_cast<unsigned char>(c)) - 'a';
            if (index < 0 || index > 25) {
                index = 26;
            }

            if (!node->children[index]) {
                node->children[index] = new TrieNode();
            }
            node = node->children[index];
        }

        node->endOfWord = true;
        node->cast.emplace_back(cast);
    }

    vector<CastRelation*> find(TitleRelation* title) const {
        vector<CastRelation*> result;
        TrieNode* node = root;

        for (auto c : title->title) {
            int index = tolower(static_cast<unsigned char>(c)) - 'a';
            if (index < 0 || index > 25) {
                index = 26;
            }

            if (node->endOfWord) {
                result.insert(result.end(), node->cast.begin(), node->cast.end());
            }

            if (!node->children[index]) {
                return result;
            }

            node = node->children[index];
        }

        if (node->endOfWord) {
            result.insert(result.end(), node->cast.begin(), node->cast.end());
        }

        return result;
    }

    static void freeTrie(TrieNode* node) {
        for (auto& child : node->children) {
            if (child != nullptr) {
                freeTrie(child);
            }
        }
        delete node;
    }
};

//-------------------------------------------------------------------------------------------------------------------------

std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation,
                                        const std::vector<TitleRelation>& titleRelation,
                                        int numThreads) {
    std::vector<ResultRelation> resultTuples;

    omp_set_num_threads(numThreads);

    Trie* trie = new Trie();
    for (const auto& cast : castRelation) {
        trie->insert(const_cast<CastRelation*>(&cast));
    }

    std::vector<std::vector<ResultRelation>> threadLocalResults(numThreads);

#pragma omp parallel
    {
        int threadId = omp_get_thread_num();
        std::vector<ResultRelation>& localResults = threadLocalResults[threadId];

#pragma omp for schedule(dynamic)
        for (const auto & title : titleRelation) {
            vector<CastRelation*> casts = trie->find(const_cast<TitleRelation*>(&title));

            for (auto element : casts) {
                localResults.emplace_back(createResultTuple(*element, title));
            }
        }
    }

    // Flatten results
    for (const auto& localVec : threadLocalResults) {
        resultTuples.insert(resultTuples.end(), localVec.begin(), localVec.end());
    }

    delete trie;
    return resultTuples;
}
