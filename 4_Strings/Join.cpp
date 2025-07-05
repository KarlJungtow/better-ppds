#include "JoinUtils.hpp"
#include <vector>
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
            int index = tolower(c) - 'a';
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
            int index = tolower(c) - 'a';
            if (index < 0 || index > 25) {
                index = 26;
            }
            //Sind im jetzigen Wort castRelation Entrys? Dann müssen sie Präfixe vom Wort sein
            if (!node->cast.empty()) {
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
    //omp_set_num_threads(numThreads);
    std::vector<ResultRelation> resultTuples;

    for(auto cast : castRelation) {
        for (auto title : titleRelation) {
            if (strncasecmp(cast.note, title.title, strlen(cast.note))==0) {
                resultTuples.push_back(createResultTuple(cast, title));
            }
        }
    }
    return resultTuples;
}