/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2016, The Souffle Developers. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

#pragma once

#include "ProgramRun.h"
#include "StringUtils.h"
#include "Table.h"
#include <memory>
#include <string>
#include <vector>

namespace souffle {
namespace profile {

/*
 * Class to format profiler data structures into tables
 */
class OutputProcessor {
private:
    std::shared_ptr<ProgramRun> programRun;

public:
    OutputProcessor() {
        programRun = std::make_shared<ProgramRun>(ProgramRun());
    }

    inline std::shared_ptr<ProgramRun>& getProgramRun() {
        return programRun;
    }

    Table getRelTable();

    Table getRulTable();

    Table getSubrulTable(std::string strRel, std::string strRul);

    Table getAtomTable(std::string strRel, std::string strRul);

    Table getVersions(std::string strRel, std::string strRul);

    Table getVersionAtoms(std::string strRel, std::string strRul, int version);

    std::string formatTime(double number) {
        return Tools::formatTime(number);
    }

    std::string formatNum(int precision, long number) {
        return Tools::formatNum(precision, number);
    }

    std::vector<std::vector<std::string>> formatTable(Table table, int precision) {
        return Tools::formatTable(table, precision);
    }
};

}  // namespace profile
}  // namespace souffle
/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2016, The Souffle Developers. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

#include "Cell.h"
#include "CellInterface.h"
#include "Iteration.h"
#include "ProgramRun.h"
#include "Relation.h"
#include "Row.h"
#include "Rule.h"
#include "Table.h"
#include <memory>
#include <unordered_map>

namespace souffle {
namespace profile {

/*
 * rel table :
 * ROW[0] = TOT_T
 * ROW[1] = NREC_T
 * ROW[2] = REC_T
 * ROW[3] = COPY_T
 * ROW[4] = TUPLES
 * ROW[5] = REL NAME
 * ROW[6] = ID
 * ROW[7] = SRC
 * ROW[8] = PERFOR
 *
 */
Table inline OutputProcessor::getRelTable() {
    std::unordered_map<std::string, std::shared_ptr<Relation>>& relation_map = programRun->getRelation_map();
    Table table;
    for (auto& rel : relation_map) {
        std::shared_ptr<Relation> r = rel.second;
        Row row(12);
        double total_time = r->getNonRecTime() + r->getRecTime() + r->getCopyTime();
        row[0] = std::shared_ptr<CellInterface>(new Cell<double>(total_time));
        row[1] = std::shared_ptr<CellInterface>(new Cell<double>(r->getNonRecTime()));
        row[2] = std::shared_ptr<CellInterface>(new Cell<double>(r->getRecTime()));
        row[3] = std::shared_ptr<CellInterface>(new Cell<double>(r->getCopyTime()));
        row[4] = std::shared_ptr<CellInterface>(new Cell<long>(r->getNum_tuplesRel()));
        row[5] = std::shared_ptr<CellInterface>(new Cell<std::string>(r->getName()));
        row[6] = std::shared_ptr<CellInterface>(new Cell<std::string>(r->getId()));
        row[7] = std::shared_ptr<CellInterface>(new Cell<std::string>(r->getLocator()));
        if (total_time != 0.0) {
            row[8] = std::shared_ptr<CellInterface>(new Cell<double>(r->getNum_tuplesRel() / total_time));
        } else {
            row[8] = std::shared_ptr<CellInterface>(new Cell<double>(r->getNum_tuplesRel() / 1.0));
        }
        row[9] = std::shared_ptr<CellInterface>(new Cell<double>(r->getSavetime()));
        row[10] = std::shared_ptr<CellInterface>(new Cell<double>(r->getLoadtime()));
        row[11] = std::shared_ptr<CellInterface>(new Cell<long>(r->getMaxRSSDiff()));

        table.addRow(std::make_shared<Row>(row));
    }
    return table;
}
/*
 * rul table :
 * ROW[0] = TOT_T
 * ROW[1] = NREC_T
 * ROW[2] = REC_T
 * ROW[3] = COPY_T
 * ROW[4] = TUPLES
 * ROW[5] = RUL NAME
 * ROW[6] = ID
 * ROW[7] = SRC
 * ROW[8] = PERFOR
 * ROW[9] = VER
 * ROW[10]= REL_NAME
 */
Table inline OutputProcessor::getRulTable() {
    std::unordered_map<std::string, std::shared_ptr<Relation>>& relation_map = programRun->getRelation_map();
    std::unordered_map<std::string, std::shared_ptr<Row>> rule_map;

    double tot_rec_tup = programRun->getTotNumRecTuples();
    double tot_copy_time = programRun->getTotCopyTime();

    for (auto& rel : relation_map) {
        for (auto& _rul : rel.second->getRuleMap()) {
            Row row(11);
            std::shared_ptr<Rule> rul = _rul.second;
            row[1] = std::shared_ptr<CellInterface>(new Cell<double>(rul->getRuntime()));
            row[2] = std::shared_ptr<CellInterface>(new Cell<double>(0.0));
            row[3] = std::shared_ptr<CellInterface>(new Cell<double>(0.0));
            row[4] = std::shared_ptr<CellInterface>(new Cell<long>(rul->getNum_tuples()));
            row[5] = std::shared_ptr<CellInterface>(new Cell<std::string>(rul->getName()));
            row[6] = std::shared_ptr<CellInterface>(new Cell<std::string>(rul->getId()));
            row[7] = std::shared_ptr<CellInterface>(new Cell<std::string>(rel.second->getName()));
            row[8] = std::shared_ptr<CellInterface>(new Cell<long>(0));
            row[10] = std::shared_ptr<CellInterface>(new Cell<std::string>(rul->getLocator()));
            rule_map.emplace(rul->getName(), std::make_shared<Row>(row));
        }
        for (auto& iter : rel.second->getIterations()) {
            for (auto& _rul : iter->getRul_rec()) {
                std::shared_ptr<Rule> rul = _rul.second;
                if (rule_map.find(rul->getName()) != rule_map.end()) {
                    std::shared_ptr<Row> _row = rule_map[rul->getName()];
                    Row row = *_row;
                    row[2] = std::shared_ptr<CellInterface>(
                            new Cell<double>(row[2]->getDoubVal() + rul->getRuntime()));
                    row[4] = std::shared_ptr<CellInterface>(
                            new Cell<long>(row[4]->getLongVal() + rul->getNum_tuples()));
                    row[0] = std::shared_ptr<CellInterface>(new Cell<double>(rul->getRuntime()));
                    rule_map[rul->getName()] = std::make_shared<Row>(row);
                } else {
                    Row row(11);
                    row[1] = std::shared_ptr<CellInterface>(new Cell<double>(0.0));
                    row[2] = std::shared_ptr<CellInterface>(new Cell<double>(rul->getRuntime()));
                    row[3] = std::shared_ptr<CellInterface>(new Cell<double>(0.0));
                    row[4] = std::shared_ptr<CellInterface>(new Cell<long>(rul->getNum_tuples()));
                    row[5] = std::shared_ptr<CellInterface>(new Cell<std::string>(rul->getName()));
                    row[6] = std::shared_ptr<CellInterface>(new Cell<std::string>(rul->getId()));
                    row[7] = std::shared_ptr<CellInterface>(new Cell<std::string>(rel.second->getName()));
                    row[8] = std::shared_ptr<CellInterface>(new Cell<long>(rul->getVersion()));
                    row[0] = std::shared_ptr<CellInterface>(new Cell<double>(rul->getRuntime()));
                    row[10] = std::shared_ptr<CellInterface>(new Cell<std::string>(rul->getLocator()));
                    rule_map[rul->getName()] = std::make_shared<Row>(row);
                }
            }
        }
        for (auto& _row : rule_map) {
            std::shared_ptr<Row> row = _row.second;
            Row t = *row;
            if (t[6]->getStringVal().at(0) == 'C') {
                double rec_tup = (double)(t[4]->getLongVal());
                t[3] = std::shared_ptr<CellInterface>(
                        new Cell<double>(rec_tup * tot_copy_time / tot_rec_tup));
            }
            double val = t[1]->getDoubVal() + t[2]->getDoubVal() + t[3]->getDoubVal();

            t[0] = std::shared_ptr<CellInterface>(new Cell<double>(val));

            if (t[0]->getDoubVal() != 0.0) {
                t[9] = std::shared_ptr<CellInterface>(
                        new Cell<double>(t[4]->getLongVal() / t[0]->getDoubVal()));
            } else {
                t[9] = std::shared_ptr<CellInterface>(new Cell<double>(t[4]->getLongVal() / 1.0));
            }
            _row.second = std::make_shared<Row>(t);
        }
    }

    Table table;
    for (auto& _row : rule_map) {
        table.addRow(_row.second);
    }
    return table;
}

/*
 * atom table :
 * ROW[0] = clause
 * ROW[1] = atom
 * ROW[2] = frequency
 */
Table inline OutputProcessor::getAtomTable(std::string strRel, std::string strRul) {
    std::unordered_map<std::string, std::shared_ptr<Relation>>& relation_map = programRun->getRelation_map();

    Table table;
    for (auto& _rel : relation_map) {
        std::shared_ptr<Relation> rel = _rel.second;

        if (rel->getId() != strRel) {
            continue;
        }

        for (auto& _rul : rel->getRuleMap()) {
            std::shared_ptr<Rule> rul = _rul.second;
            if (rul->getId() != strRul) {
                continue;
            }
            for (auto& atom : rul->getAtoms()) {
                Row row(3);
                row[0] = std::shared_ptr<CellInterface>(new Cell<std::string>(std::get<0>(atom.first)));
                row[1] = std::shared_ptr<CellInterface>(new Cell<std::string>(std::get<1>(atom.first)));
                row[2] = std::shared_ptr<CellInterface>(new Cell<long>(atom.second));

                table.addRow(std::make_shared<Row>(row));
            }
        }
    }
    return table;
}

/*
 * subrule table :
 * ROW[0] = subrule
 */
Table inline OutputProcessor::getSubrulTable(std::string strRel, std::string strRul) {
    std::unordered_map<std::string, std::shared_ptr<Relation>>& relation_map = programRun->getRelation_map();

    Table table;
    for (auto& _rel : relation_map) {
        std::shared_ptr<Relation> rel = _rel.second;

        if (rel->getId() != strRel) {
            continue;
        }

        for (auto& _rul : rel->getRuleMap()) {
            std::shared_ptr<Rule> rul = _rul.second;
            if (rul->getId() != strRul) {
                continue;
            }
            for (auto& atom : rul->getAtoms()) {
                Row row(1);
                row[0] = std::shared_ptr<CellInterface>(new Cell<std::string>(std::get<0>(atom.first)));

                table.addRow(std::make_shared<Row>(row));
            }
        }
    }
    return table;
}

/*
 * ver table :
 * ROW[0] = TOT_T
 * ROW[1] = NREC_T
 * ROW[2] = REC_T
 * ROW[3] = COPY_T
 * ROW[4] = TUPLES
 * ROW[5] = RUL NAME
 * ROW[6] = ID
 * ROW[7] = SRC
 * ROW[8] = PERFOR
 * ROW[9] = VER
 * ROW[10]= REL_NAME
 */
Table inline OutputProcessor::getVersions(std::string strRel, std::string strRul) {
    std::unordered_map<std::string, std::shared_ptr<Relation>>& relation_map = programRun->getRelation_map();
    std::unordered_map<std::string, std::shared_ptr<Row>> rule_map;

    double tot_rec_tup = programRun->getTotNumRecTuples();
    double tot_copy_time = programRun->getTotCopyTime();

    for (auto& _rel : relation_map) {
        std::shared_ptr<Relation> rel = _rel.second;
        if (rel->getId().compare(strRel) == 0) {
            for (auto& iter : rel->getIterations()) {
                for (auto& _rul : iter->getRul_rec()) {
                    std::shared_ptr<Rule> rul = _rul.second;
                    if (rul->getId().compare(strRul) == 0) {
                        std::string strTemp =
                                rul->getName() + rul->getLocator() + std::to_string(rul->getVersion());

                        if (rule_map.find(strTemp) != rule_map.end()) {
                            std::shared_ptr<Row> _row = rule_map[strTemp];
                            Row row = *_row;
                            row[2] = std::shared_ptr<CellInterface>(
                                    new Cell<double>(row[2]->getDoubVal() + rul->getRuntime()));
                            row[4] = std::shared_ptr<CellInterface>(
                                    new Cell<long>(row[4]->getLongVal() + rul->getNum_tuples()));
                            row[0] = std::shared_ptr<CellInterface>(new Cell<double>(rul->getRuntime()));
                            rule_map[strTemp] = std::make_shared<Row>(row);
                        } else {
                            Row row(10);
                            row[1] = std::shared_ptr<CellInterface>(new Cell<double>(0.0));
                            row[2] = std::shared_ptr<CellInterface>(new Cell<double>(rul->getRuntime()));
                            row[4] = std::shared_ptr<CellInterface>(new Cell<long>(rul->getNum_tuples()));
                            row[5] = std::shared_ptr<CellInterface>(new Cell<std::string>(rul->getName()));
                            row[6] = std::shared_ptr<CellInterface>(new Cell<std::string>(rul->getId()));
                            row[7] = std::shared_ptr<CellInterface>(new Cell<std::string>(rel->getName()));
                            row[8] = std::shared_ptr<CellInterface>(new Cell<long>(rul->getVersion()));
                            row[9] = std::shared_ptr<CellInterface>(new Cell<std::string>(rul->getLocator()));
                            row[0] = std::shared_ptr<CellInterface>(new Cell<double>(rul->getRuntime()));
                            rule_map[strTemp] = std::make_shared<Row>(row);
                        }
                    }
                }
            }
            for (auto row : rule_map) {
                Row t = *row.second;
                double d = tot_rec_tup;
                long d2 = t[4]->getLongVal();
                t[3] = std::shared_ptr<CellInterface>(new Cell<double>(d2 * tot_copy_time / d));
                t[0] = std::shared_ptr<CellInterface>(
                        new Cell<double>(t[1]->getDoubVal() + t[2]->getDoubVal() + t[3]->getDoubVal()));
                rule_map[row.first] = std::make_shared<Row>(t);
            }
            break;
        }
    }

    Table table;
    for (auto& _row : rule_map) {
        table.addRow(_row.second);
    }
    return table;
}

/*
 * atom table :
 * ROW[0] = rule
 * ROW[1] = atom
 * ROW[3] = frequency
 */
Table inline OutputProcessor::getVersionAtoms(std::string strRel, std::string srcLocator, int version) {
    std::unordered_map<std::string, std::shared_ptr<Relation>>& relation_map = programRun->getRelation_map();
    Table table;

    for (auto& _rel : relation_map) {
        std::shared_ptr<Relation> rel = _rel.second;
        if (rel->getId().compare(strRel) == 0) {
            for (auto& iter : rel->getIterations()) {
                for (auto& _rul : iter->getRul_rec()) {
                    std::shared_ptr<Rule> rul = _rul.second;
                    if (rul->getLocator().compare(srcLocator) == 0 && rul->getVersion() == version) {
                        for (auto& atom : rul->getAtoms()) {
                            Row row(4);
                            row[0] = std::shared_ptr<CellInterface>(
                                    new Cell<std::string>(std::get<0>(atom.first)));
                            row[1] = std::shared_ptr<CellInterface>(
                                    new Cell<std::string>(std::get<1>(atom.first)));
                            row[2] = std::shared_ptr<CellInterface>(new Cell<long>(atom.second));
                            table.addRow(std::make_shared<Row>(row));
                        }
                    }
                }
            }
            break;
        }
    }

    return table;
}

}  // namespace profile
}  // namespace souffle
