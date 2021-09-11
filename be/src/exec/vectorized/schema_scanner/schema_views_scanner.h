// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

namespace vectorized {

class SchemaViewsScanner : public SchemaScanner {
public:
    SchemaViewsScanner();
    virtual ~SchemaViewsScanner();

    Status start(RuntimeState* state) override;
    Status get_next_row(ChunkPtr* chunk, bool *eos) override;

private:
    Status get_new_table();
    Status fill_one_row(ChunkPtr* chunk);

    int _db_index;
    int _table_index;
    TGetDbsResult _db_result;
    TListTableStatusResult _table_result;
    static SchemaScanner::ColumnDesc _s_tbls_columns[];
};

} // namespace vectorized

} // namespace starrocks