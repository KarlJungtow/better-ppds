#include "JoinUtils.hpp"
#include <vector>
#include <memory>
#include <cctype>
#include <string_view>
#include <string_view>
using namespace std;

// Konstanten für Trie-Konfiguration
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

            // Wenn Node noch nicht existiert, kreiere sie
            if (!node->children[index]) {
                node->children[index] = make_unique<TrieNode>();
            }

            // Gehe zur nächsten Node
            node = node->children[index].get();
        }

        // Setze am Ende Cast als pointer hin
        node->endOfWord = true;
        node->cast.push_back(cast);
    }

    void findPrefixMatches(const string& prefix, vector<const CastRelation*>& results) const {
        TrieNode* node = root.get();

        for (char c : prefix) {
            // Füge alle passenden Einträge auf aktuellem Knoten hinzu
            if (node->endOfWord) {
                for (auto element : node->cast) {
                    results.emplace_back(element);
                }
            }

            int index = charToIndex(c);
            if (!node->children[index]) return;

            node = node->children[index].get();
        }

        // Füge Einträge des exakten Endknotens hinzu
        if (node->endOfWord) {
            for (auto element : node->cast) {
                results.emplace_back(element);
            }
        }
    }

    void printTrie() const {
        string currentPrefix;
        printTrieRecursive(root.get(), currentPrefix);
    }

private:
    void printTrieRecursive(const TrieNode* node, string& prefix) const {
        if (!node) return;

        if (node->endOfWord) {
            // Print the prefix and optionally additional cast information
            cout << "Word: " << prefix << " | Cast count: " << node->cast.size() << '\n';
        }

        for (int i = 0; i < TOTAL_CHILDREN; ++i) {
            if (node->children[i]) {
                char c;
                if (i == OTHER_INDEX) {
                    c = '?';  // or some placeholder for non-alphabetic characters
                } else {
                    c = static_cast<char>('a' + i);
                }

                prefix.push_back(c);
                printTrieRecursive(node->children[i].get(), prefix);
                prefix.pop_back(); // backtrack
            }
        }
    }

};

//-------------------------------------------------------------------------------------------------------------------------

vector<ResultRelation> performJoin(const vector<CastRelation>& castRelation,
                                    const vector<TitleRelation>& titleRelation,
                                    int numThreads) {
    vector<ResultRelation> resultTuples;
    resultTuples.reserve(titleRelation.size() * 2); // Heuristische Reserve

    Trie trie;

    // Trie mit Cast-Daten füllen
    for (const auto& cast : castRelation) {
        trie.insert(&cast);
    }
    trie.printTrie();
    // Titel durchsuchen und Matches sammelncd
    vector<const CastRelation*> prefixMatches;
    for (const auto& title : titleRelation) {
        prefixMatches.clear();
        trie.findPrefixMatches(title.title, prefixMatches);

        for (const auto cast : prefixMatches) {
            resultTuples.emplace_back(createResultTuple(*cast, title));
        }
    }

    return resultTuples;
}