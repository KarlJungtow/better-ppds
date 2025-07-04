#include "JoinUtils.hpp"
#include <vector>
#include <chrono>
using namespace std;


class TrieNode {
public:
    bool endOfWord;
    TrieNode* children[26]{};
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
            int index = c - 'a';
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

    static void freeTrie(TrieNode* node) {
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
    Trie* trie = new Trie();
    for (auto cast : castRelation) {
        trie->insert(&cast);
    }

    for (auto title : titleRelation) {
        vector<CastRelation*> casts = trie->find(&title);
        for (auto cast : casts) {
            resultTuples.emplace_back(createResultTuple(*cast, title));
        }
    }
    free(trie);
    return resultTuples;
}


TitleRelation createTitleRelation(int32_t titleId, const char* title, const char* imdbIndex,
                                  int32_t kindId, int32_t productionYear, int32_t imdbId,
                                  const char* phoneticCode, const char* seriesYears, const char* md5sum) {
    TitleRelation rel{};
    rel.titleId = titleId;
    std::strncpy(rel.title, title, sizeof(rel.title));
    std::strncpy(rel.imdbIndex, imdbIndex, sizeof(rel.imdbIndex));
    rel.kindId = kindId;
    rel.productionYear = productionYear;
    rel.imdbId = imdbId;
    std::strncpy(rel.phoneticCode, phoneticCode, sizeof(rel.phoneticCode));
    rel.episodeOfId = 0;
    rel.seasonNr = 0;
    rel.episodeNr = 0;
    std::strncpy(rel.seriesYears, seriesYears, sizeof(rel.seriesYears));
    std::strncpy(rel.md5sum, md5sum, sizeof(rel.md5sum));
    return rel;
}

std::vector<TitleRelation> getHardcodedTitleRelations() {
    return {
        createTitleRelation(1, "The Matrix", "I1", 1, 1999, 101, "TMX", "1999", "md5sum000000000000000000000000000001"),
        createTitleRelation(2, "Inception", "I2", 1, 2010, 102, "INC", "2010", "md5sum000000000000000000000000000002"),
        createTitleRelation(3, "The Godfather", "I3", 1, 1972, 103, "TGF", "1972", "md5sum000000000000000000000000000003"),
        createTitleRelation(4, "Interstellar", "I4", 1, 2014, 104, "IST", "2014", "md5sum000000000000000000000000000004"),
        createTitleRelation(5, "Pulp Fiction", "I5", 1, 1994, 105, "PFN", "1994", "md5sum000000000000000000000000000005"),
        createTitleRelation(6, "The Dark Knight", "I6", 1, 2008, 106, "TDK", "2008", "md5sum000000000000000000000000000006"),
        createTitleRelation(7, "Fight Club", "I7", 1, 1999, 107, "FCB", "1999", "md5sum000000000000000000000000000007"),
        createTitleRelation(8, "Forrest Gump", "I8", 1, 1994, 108, "FGP", "1994", "md5sum000000000000000000000000000008"),
        createTitleRelation(9, "The Shawshank Redemption", "I9", 1, 1994, 109, "TSR", "1994", "md5sum000000000000000000000000000009"),
        createTitleRelation(10, "The Lord of the Rings", "I10", 1, 2001, 110, "LOTR", "2001", "md5sum000000000000000000000000000010")
    };
}
CastRelation createCastRelation(int32_t castInfoId, int32_t personId, int32_t movieId,
                                int32_t personRoleId, const char* note, int32_t nrOrder, int32_t roleId) {
    CastRelation rel{};
    rel.castInfoId = castInfoId;
    rel.personId = personId;
    rel.movieId = movieId;
    rel.personRoleId = personRoleId;
    std::strncpy(rel.note, note, sizeof(rel.note));
    rel.nrOrder = nrOrder;
    rel.roleId = roleId;
    return rel;
}

std::vector<CastRelation> getHardcodedCastRelations() {
    return {
        createCastRelation(1001, 201, 1, 301, "Neo", 1, 401),
        createCastRelation(1002, 202, 2, 302, "Dom Cobb", 1, 402),
        createCastRelation(1003, 203, 3, 303, "Don Vito", 1, 403),
        createCastRelation(1004, 204, 4, 304, "Cooper", 1, 404),
        createCastRelation(1005, 205, 5, 305, "Jules", 1, 405),
        createCastRelation(1006, 206, 6, 306, "Batman", 1, 406),
        createCastRelation(1007, 207, 7, 307, "Tyler Durden", 1, 407),
        createCastRelation(1008, 208, 8, 308, "Forrest", 1, 408),
        createCastRelation(1009, 209, 9, 309, "Andy", 1, 409),
        createCastRelation(1010, 210, 10, 310, "Frodo", 1, 410)
    };
}



int main() {
    std::cout << "Test reading data from a file.\n";

    // Load relations
    const auto rightRelation = getHardcodedTitleRelations();
    const auto leftRelation = getHardcodedCastRelations();
    // Time the join
    auto start = std::chrono::high_resolution_clock::now();
    const auto resultTuples = performJoin(leftRelation, rightRelation, 8);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsedSeconds = end - start;

    // Output timing
    std::cout << "\nJoin operation took " << elapsedSeconds.count() << " seconds.\n";

    // Output results
    std::cout << "\n\n#######################################\n";
    std::cout << "############### RESULTS ###############\n";
    std::cout << "#######################################\n";
    for (const auto& resultTuple : resultTuples) {
        std::cout << resultRelationToString(resultTuple) << '\n';
    }
    std::cout << "\n\n";

    return 0;
}