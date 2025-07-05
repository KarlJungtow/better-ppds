#include "JoinUtils.hpp"
#include <vector>
#include <memory>
#include <cctype>
#include <algorithm>
#include <unordered_map>
#include <functional>
#include <string_view>
using namespace std;

// Konstanten für Trie-Konfiguration
static constexpr int ALPHABET_SIZE = 26;
static constexpr int OTHER_INDEX = ALPHABET_SIZE;
static constexpr int TOTAL_CHILDREN = ALPHABET_SIZE + 1;

// Hilfsfunktion für sichere Zeichenkonvertierung
inline char safe_to_lower(char c) noexcept {
    return static_cast<char>(tolower(static_cast<unsigned char>(c)));
}

class Trie {
private:
    struct TrieNode {
        bool endOfWord = false;
        vector<const CastRelation*> cast;
        unique_ptr<TrieNode> children[TOTAL_CHILDREN];
    };

    unique_ptr<TrieNode> root;

    int charToIndex(char c) const noexcept {
        c = safe_to_lower(c);
        if (c >= 'a' && c <= 'z')
            return c - 'a';
        return OTHER_INDEX;
    }

public:
    Trie() : root(make_unique<TrieNode>()) {}

    void insert(const CastRelation* cast) {
        TrieNode* node = root.get();
        //Nutzen string_view statt string für Performance, da es keine Kopie erstellt
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
                results.insert(results.end(), node->cast.begin(), node->cast.end());
            }

            int index = charToIndex(c);
            if (!node->children[index]) return;

            node = node->children[index].get();
        }

        // Füge Einträge des exakten Endknotens hinzu
        if (node->endOfWord) {
            results.insert(results.end(), node->cast.begin(), node->cast.end());
        }
    }
};

//-------------------------------------------------------------------------------------------------------------------------

vector<ResultRelation> performJoin2(const vector<CastRelation>& castRelation,
                                    const vector<TitleRelation>& titleRelation,
                                    int numThreads) {
    vector<ResultRelation> resultTuples;
    resultTuples.reserve(titleRelation.size() * 2); // Heuristische Reserve

    Trie trie;

    // Trie mit Cast-Daten füllen
    for (const auto& cast : castRelation) {
        trie.insert(&cast);
    }

    // Titel durchsuchen und Matches sammeln
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

vector<ResultRelation> performJoin(const vector<CastRelation>& castRelation,
                                   const vector<TitleRelation>& titleRelation,
                                   int numThreads) {
    vector<ResultRelation> resultTuples;
    resultTuples.reserve(castRelation.size()); // Konservative Schätzung

    for (const auto& cast : castRelation) {
        const string_view note(cast.note);
        const size_t noteLength = note.length();

        for (const auto& title : titleRelation) {
            const string_view titleStr(title.title);

            // Vermeidet Buffer-Overflow und prüft Präfix
            if (noteLength > titleStr.length()) continue;

            bool match = true;
            for (size_t i = 0; i < noteLength; ++i) {
                if (safe_to_lower(note[i]) != safe_to_lower(titleStr[i])) {
                    match = false;
                    break;
                }
            }

            if (match) {
                resultTuples.emplace_back(createResultTuple(cast, title));
            }
        }
    }

    return resultTuples;
}

std::vector<ResultRelation> performJoin3(const std::vector<CastRelation>& castRelation,
                                        const std::vector<TitleRelation>& titleRelation,
                                        int numThreads) {
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