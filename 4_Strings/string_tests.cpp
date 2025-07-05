#include "JoinUtils.hpp"
#include "Join.hpp"
#include <iostream>
#include <cassert>
#include <vector>

int main() {
    // Testdaten erstellen
    std::vector<CastRelation> castRelations = {
        {1, 100, 200, 300, "hello", 1, 400},
        {2, 101, 201, 301, "world", 2, 401},
        {3, 102, 202, 302, "prefix", 3, 402},
        {4, 103, 203, 303, "test", 4, 403}
    };

    std::vector<TitleRelation> titleRelations = {
        {1, "hello world", "", 1, 2020, 1, "", 0, 0, 0, "", ""},
        {2, "prefix match", "", 2, 2021, 2, "", 0, 0, 0, "", ""},
        {3, "no match", "", 3, 2022, 3, "", 0, 0, 0, "", ""},
        {4, "hello", "", 4, 2023, 4, "", 0, 0, 0, "", ""}
    };

    // Join durchführen
    std::vector<ResultRelation> results = performJoin(castRelations, titleRelations, 1);

    // Debug-Ausgabe
    std::cout << "Anzahl Ergebnisse: " << results.size() << "\n";
    for (const auto& res : results) {
        std::cout << "Title: " << res.title
                  << " | Note: " << res.note
                  << " | TitleID: " << res.titleId
                  << " | CastID: " << res.castInfoId << "\n";
    }

    // Assertions für erwartete Ergebnisse
    assert(results.size() == 5);  // Erwartete Anzahl von Ergebnissen

    bool found1 = false, found2 = false, found3 = false, found4 = false, found5 = false;
    for (const auto& res : results) {
        if (res.titleId == 1 && res.castInfoId == 1) found1 = true;  // hello world - hello
        if (res.titleId == 1 && res.castInfoId == 2) found2 = true;  // hello world - world
        if (res.titleId == 2 && res.castInfoId == 3) found3 = true;  // prefix match - prefix
        if (res.titleId == 4 && res.castInfoId == 1) found4 = true;  // hello - hello
        if (res.titleId == 4 && res.castInfoId == 3) found5 = true;  // hello - prefix (sollte nicht passen!)
    }

    assert(found1);
    assert(found2);
    assert(found3);
    assert(found4);
    assert(!found5);  // Sollte false sein!

    std::cout << "Alle Tests erfolgreich bestanden!\n";
    return 0;
}