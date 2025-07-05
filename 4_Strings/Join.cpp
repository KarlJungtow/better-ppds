#include "JoinUtils.hpp"
#include <vector>
#include <omp.h>
using namespace std;

class TrieNode {
public:
    bool endOfWord;
    TrieNode* children[27]{};
    vector<CastRelation*> cast;
    TrieNode() {
        endOfWord = false;
        for(auto & child : children) {
            child = nullptr;
        }
        cast = {};
    }
};

class Trie {
private:
    TrieNode* root;

public:
    Trie() {root = new TrieNode();}
    ~Trie() {
        freeTrie(root);
    }
    void insert(CastRelation* cast) const {
        TrieNode* node = root;
        for(auto c : cast->note) {
            int index = tolower(static_cast<unsigned char>(c)) - 'a';
            if (index < 0 || index > 25) {
                index = 26;
            }
            if(!node->children[index]) {
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

        // Für Buchstaben im Wort
        for(auto c : title->title) {

            //Welches Child ist es?
            int index = tolower(static_cast<unsigned char>(c)) - 'a';
            if (index < 0 || index > 25) {
                index = 26;
            }
            //Sind im jetzigen Wort castRelation Entrys? Dann müssen sie Präfixe vom Wort sein
            if (node->endOfWord) {
                for (auto cast_entry : node->cast) {
                    result.emplace_back(cast_entry);
                }
            }
            //Wort hört hier auf? Returne
            if(!node->children[index]) {
                return result;
            }
            // Else: Gehe weiter
            node = node->children[index];
        }

        // Prüfe explizit den letzten Knoten (für exakte Übereinstimmungen)
        if (node->endOfWord) {
            for (auto cast_entry : node->cast) {
                result.push_back(cast_entry);
            }
        }

        return result;
    }

    static void  freeTrie(TrieNode* node) {
        for (auto & i : node->children) {
            if (i != nullptr) {
                freeTrie(i);
            }
        }
        delete node;
    }

};

//-------------------------------------------------------------------------------------------------------------------------
std::vector<ResultRelation> performJoin(const std::vector<CastRelation>& castRelation, const std::vector<TitleRelation>& titleRelation, int numThreads) {
    omp_set_num_threads(numThreads);
    vector<ResultRelation> resultTuples;
    Trie* trie = new Trie();
    for (const auto& cast : castRelation) {
        trie->insert(const_cast<CastRelation*>(&cast));
    }

    vector<vector<ResultRelation>> threadLocalResults(numThreads);

#pragma omp parallel num_threads(numThreads)
    {
        vector<ResultRelation>& localResults = threadLocalResults[numThreads];

#pragma omp for schedule(dynamic)
        for (auto title : titleRelation) {
            vector<CastRelation*> casts = trie->find(&title);

            for (auto element : casts) {
                localResults.emplace_back(createResultTuple(*element, title));
            }
        }

        // Flatten threadLocalResults into resultTuples
        for (auto& localVec : threadLocalResults) {
            resultTuples.insert(resultTuples.end(), localVec.begin(), localVec.end());
        }
    }
    
    delete trie;
    return resultTuples;
}