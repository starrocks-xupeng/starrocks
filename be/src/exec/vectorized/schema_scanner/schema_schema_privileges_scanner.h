// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <string>

#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

namespace vectorized {

class SchemaSchemaPrivilegesScanner : public SchemaScanner {
public:
    SchemaSchemaPrivilegesScanner();
    virtual ~SchemaSchemaPrivilegesScanner();
    Status start(RuntimeState* state) override;
    Status get_next_row(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_one_row(ChunkPtr* chunk);

    int _db_priv_index;
    TGetDBPrivsResult _db_privs_result;
    static SchemaScanner::ColumnDesc _s_db_privs_columns[];
};

} // namespace vectorized

} // namespace starrocks