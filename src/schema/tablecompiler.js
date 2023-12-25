// ClicHouse Table Builder & Compiler
// -------
const TableCompiler = require("knex/lib/schema/tablecompiler");
// Table Compiler
// ------

class TableCompilerClickHouse extends TableCompiler {
    addColumnsPrefix = "ADD COLUMN ";

    alterColumnsPrefix = "modify COLUMN ";

    dropColumnPrefix = "drop COLUMN ";

    createQuery(columns, ifNot) {
        const createStatement = ifNot ? "create table if not exists " : "create table ";
        let sql = createStatement + this.tableName() + " (" + columns.sql.join(", ") + ")";

        const engine = this.single.engine || "MergeTree";

        if (engine) sql += ` engine = ${engine}`;

        if (this.single.comment) {
            const comment = this.single.comment || "";
            if (comment.length > 60) this.client.logger.warn("The max length for a table comment is 60 characters");
            sql += ` comment = '${comment}'`;
        }

        if (
            ![
                "MergeTree",
                "ReplacingMergeTree",
                "SummingMergeTree",
                "AggregatingMergeTree",
                "CollapsingMergeTree",
            ].includes(engine)
        ) {
            // skip
        } else if (this.grouped.alterTable && this.grouped.alterTable.length > 0) {
            sql += ` ORDER BY (${this.grouped.alterTable.map((row) => row.args[0]).join(",")})`;
        } else {
            sql += ` ORDER BY tuple()`;
        }
        console.log("TableCompilerClickHouse.createQuery:", sql);
        this.pushQuery(sql);
    }

    // Compiles the comment on the table.
    comment(comment) {
        this.pushQuery(`alter table ${this.tableName()} comment = '${comment}'`);
    }

    changeType() {
        // alter table + table + ' modify ' + wrapped + '// type';
    }

    // Renames a column on the table.
    renameColumn(from, to) {
        const compiler = this;
        const table = this.tableName();
        const wrapped = this.formatter.wrap(from) + " " + this.formatter.wrap(to);

        this.pushQuery({
            sql: `show fields from ${table} where field = ` + this.formatter.parameter(from),
            output(resp) {
                const column = resp[0];
                const runner = this;
                return compiler.getFKRefs(runner).then(([refs]) =>
                    new Promise((resolve, reject) => {
                        try {
                            if (!refs.length) {
                                resolve(null);
                            }
                            resolve(compiler.dropFKRefs(runner, refs));
                        } catch (e) {
                            reject(e);
                        }
                    })
                        .then(function f() {
                            let sql = `alter table ${table} change ${wrapped} ${column.Type}`;

                            if (String(column.Null).toUpperCase() !== "YES") {
                                sql += " NOT NULL";
                            } else {
                                // This doesn't matter for most cases except Timestamp, where this is important
                                sql += " NULL";
                            }
                            if (column.Default) {
                                sql += ` DEFAULT '${column.Default}'`;
                            }

                            return runner.query({
                                sql,
                            });
                        })
                        .then(function f() {
                            if (!refs.length) {
                                return undefined;
                            }
                            return compiler.createFKRefs(
                                runner,
                                refs.map(function m(ref) {
                                    if (ref.REFERENCED_COLUMN_NAME === from) {
                                        ref.REFERENCED_COLUMN_NAME = to;
                                    }
                                    if (ref.COLUMN_NAME === from) {
                                        ref.COLUMN_NAME = to;
                                    }
                                    return ref;
                                })
                            );
                        })
                );
            },
        });
    }

    // index(columns, indexName, indexType) {
    //     indexName = indexName
    //         ? this.formatter.wrap(indexName)
    //         : this._indexCommand("index", this.tableNameRaw, columns);
    //     this.pushQuery(
    //         `alter table ${this.tableName()} add${
    //             indexType ? ` ${indexType}` : ""
    //         } index ${indexName}(${this.formatter.columnize(columns)})`
    //     );
    // }

    // Compile a drop index command.
    dropIndex(columns, indexName) {
        indexName = indexName
            ? this.formatter.wrap(indexName)
            : this._indexCommand("index", this.tableNameRaw, columns);
        this.pushQuery(`alter table ${this.tableName()} drop index ${indexName}`);
    }
}

module.exports = TableCompilerClickHouse;
