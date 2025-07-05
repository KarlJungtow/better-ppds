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
        createCastRelation(1010, 210, 10, 310, "Frodo", 1, 410),
        // Neue CastRelations
        createCastRelation(1011, 211, 11, 311, "Neo", 1, 411),
        createCastRelation(1012, 212, 12, 312, "Vin Diesel", 1, 412),
        createCastRelation(1013, 213, 13, 313, "Hobbes", 1, 413),
        createCastRelation(1014, 214, 14, 314, "Buzz", 1, 414),
        createCastRelation(1015, 215, 15, 315, "Woody", 1, 415),
        createCastRelation(1016, 216, 16, 316, "Simba", 1, 416),
        createCastRelation(1017, 217, 17, 317, "Mufasa", 1, 417),
        createCastRelation(1018, 218, 18, 318, "Elsa", 1, 418),
        createCastRelation(1019, 219, 19, 319, "Anna", 1, 419),
        createCastRelation(1020, 220, 20, 320, "Fro", 1, 420)
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
        createTitleRelation(1, "Neeo% Matrix", "I1", 1, 1999, 101, "TMX", "1999", "md5sum0001"),
        createTitleRelation(12, "Neeo$ Matrix", "I1", 1, 1999, 101, "TMX", "1999", "md5sum0001"),
        createTitleRelation(2, "Inception", "I2", 1, 2010, 102, "INC", "2010", "md5sum0002"),
        createTitleRelation(3, "Forreests", "I3", 1, 1972, 103, "TGF", "1972", "md5sum0003"),
        createTitleRelation(4, "CAT!", "I4", 1, 2014, 104, "IST", "2014", "md5sum0004"),
        createTitleRelation(5, "CAT?", "I5", 1, 1994, 105, "PFN", "1994", "md5sum0005"),
        createTitleRelation(6, "The Dark Knight", "I6", 1, 2008, 106, "TDK", "2008", "md5sum0006"),
        createTitleRelation(7, "Fight Club", "I7", 1, 1999, 107, "FCB", "1999", "md5sum0007"),
        createTitleRelation(8, "Forreest Gump", "I8", 1, 1994, 108, "FGP", "1994", "md5sum0008"),
        createTitleRelation(9, "The Shawshank Redemption", "I9", 1, 1994, 109, "TSR", "1994", "md5sum0009"),
        createTitleRelation(10, "The Lord of the Rings", "I10", 1, 2001, 110, "LOTR", "2001", "md5sum0010"),
        createTitleRelation(11, "Forres", "I5", 1, 1994, 105, "PFN", "1994", "md5sum0011"),
        // Neue Titel
        createTitleRelation(13, "The Matrix", "I11", 1, 1999, 111, "MAT", "1999", "md5sum0012"),
        createTitleRelation(14, "Fast & Furious", "I12", 1, 2001, 112, "FNF", "2001", "md5sum0013"),
        createTitleRelation(15, "Calvin and Hobbes", "I13", 1, 1995, 113, "CNH", "1995", "md5sum0014"),
        createTitleRelation(16, "Toy Story", "I14", 1, 1995, 114, "TSY", "1995", "md5sum0015"),
        createTitleRelation(17, "The Lion King", "I15", 1, 1994, 115, "TLK", "1994", "md5sum0016"),
        createTitleRelation(18, "Frozen", "I16", 1, 2013, 116, "FRZ", "2013", "md5sum0017")
    };
}


int safe_to_lower(char c) noexcept {
    char d = static_cast<char>(tolower(static_cast<unsigned char>(c)));
    if (d >= 'a' && d <= 'z')
        return d - 'a';
    return 27;

}

int main() {
    std::cout << safe_to_lower('%') << std::endl;
    std::cout << safe_to_lower('&') << std::endl;
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
    assert(results.size() == 6);  // Erwartete Anzahl von Ergebnissen

    std::cout << "Alle Tests erfolgreich bestanden!\n";
    return 0;
}
