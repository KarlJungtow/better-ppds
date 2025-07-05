#include "JoinUtils.hpp"

#include <vector>
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

            // If this node ends a word, include all cast entries
            if (node->endOfWord) {
                result.insert(result.end(), node->cast.begin(), node->cast.end());
            }

            if (!node->children[index]) {
                return result;
            }

            node = node->children[index];
        }

        // Check the last node for exact match
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

    Trie* trie = new Trie();

    for (const auto& cast : castRelation) {
        trie->insert(const_cast<CastRelation*>(&cast));
    }

    for (const auto& title : titleRelation) {
        vector<CastRelation*> casts = trie->find(const_cast<TitleRelation*>(&title));
        for (auto element : casts) {
            resultTuples.emplace_back(createResultTuple(*element, title));
        }
    }

    delete trie;
    return resultTuples;
}
