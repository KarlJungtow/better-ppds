#include "JoinUtils.hpp"
#include "Join.hpp"
#include <iostream>
#include <cassert>
#include <vector>
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
        createCastRelation(1001, 201, 1, 301, "Neeo$", 1, 401),
        createCastRelation(1001, 201, 1, 301, "Neeo&", 1, 401),
        createCastRelation(1002, 202, 2, 302, "CAT!", 1, 402),
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
        createTitleRelation(1, "Neeo% Matrix", "I1", 1, 1999, 101, "TMX", "1999", "md5sum000000000000000000000000000001"),
        createTitleRelation(12, "Neeo$ Matrix", "I1", 1, 1999, 101, "TMX", "1999", "md5sum000000000000000000000000000001"),
        createTitleRelation(2, "Inception", "I2", 1, 2010, 102, "INC", "2010", "md5sum000000000000000000000000000002"),
        createTitleRelation(3, "Forreests", "I3", 1, 1972, 103, "TGF", "1972", "md5sum000000000000000000000000000003"),
        createTitleRelation(4, "CAT!", "I4", 1, 2014, 104, "IST", "2014", "md5sum000000000000000000000000000004"),
        createTitleRelation(5, "CAT?", "I5", 1, 1994, 105, "PFN", "1994", "md5sum000000000000000000000000000005"),
        createTitleRelation(6, "The Dark Knight", "I6", 1, 2008, 106, "TDK", "2008", "md5sum000000000000000000000000000006"),
        createTitleRelation(7, "Fight Club", "I7", 1, 1999, 107, "FCB", "1999", "md5sum000000000000000000000000000007"),
        createTitleRelation(8, "Forreest Gump", "I8", 1, 1994, 108, "FGP", "1994", "md5sum000000000000000000000000000008"),
        createTitleRelation(9, "The Shawshank Redemption", "I9", 1, 1994, 109, "TSR", "1994", "md5sum000000000000000000000000000009"),
        createTitleRelation(10, "The Lord of the Rings", "I10", 1, 2001, 110, "LOTR", "2001", "md5sum000000000000000000000000000010"),
        createTitleRelation(11, "Forres", "I5", 1, 1994, 105, "PFN", "1994", "md5sum000000000000000000000000000005"),

    };
}

int main() {
    // Join durchführen
    std::vector<CastRelation> castRelations = getHardcodedCastRelations();
    std::vector<TitleRelation> titleRelations = getHardcodedTitleRelations();
    std::vector<ResultRelation> results = performJoin(castRelations, titleRelations, 1);

    // Debug-Ausgabe
    std::cout << "Anzahl Ergebnisse: " << results.size() << "\n";
    for (const auto& res : results) {
        std::cout << "Title: " << res.title
                  << " | Note: " << res.note << "\n";
    }

    // Assertions für erwartete Ergebnisse
    assert(results.size() == 1);  // Erwartete Anzahl von Ergebnissen

    std::cout << "Alle Tests erfolgreich bestanden!\n";
    return 0;
}
