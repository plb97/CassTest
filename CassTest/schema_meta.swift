//
//  schema_meta.swift
//  CassTest
//
//  Created by Philippe on 27/12/2017.
//  Copyright © 2017 PLHB. All rights reserved.
//

//import Foundation

import Cass

fileprivate
let KEY = "test"

fileprivate
func getSession() -> Session {
    let session = Session()
    session.connect(Cluster().setContactPoints("127.0.0.1").setCredentials()).wait().check()
    return session
}

fileprivate
func create_keyspace(session: Session) -> () {
    print("create_keyspace...")
    let query = """
    CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {
                           'class': 'SimpleStrategy', 'replication_factor': '3' };
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_keyspace")
    future.check()
}
fileprivate
func create_table(session: Session) -> () {
    print("create_table...")
    let query = """
    CREATE TABLE IF NOT EXISTS examples.schema_meta (key text,
                  value bigint,
                  PRIMARY KEY (key));
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_table")
    future.check()
}
fileprivate
func create_functions(session: Session) -> () {
    print("create_functions...")
    var query = """
    CREATE OR REPLACE FUNCTION examples.avg_state(state tuple<int, bigint>, val int)
                  CALLED ON NULL INPUT RETURNS tuple<int, bigint>
                  LANGUAGE java AS
                    'if (val != null) {
                      state.setInt(0, state.getInt(0) + 1);
                      state.setLong(1, state.getLong(1) + val.intValue());
                    }
                    return state;'
                  ;
    """
    var future = session.execute(SimpleStatement(query)).wait()
    future.check()
    query = """
    CREATE OR REPLACE FUNCTION examples.avg_state(state tuple<int, bigint>, val int)
    CALLED ON NULL INPUT RETURNS tuple<int, bigint>
    LANGUAGE java AS
    'if (val != null) {
    state.setInt(0, state.getInt(0) + 1);
    state.setLong(1, state.getLong(1) + val.intValue());
    }
    return state;'
    ;
    """
    future = session.execute(SimpleStatement(query)).wait()
    future.check()
    query = """
    CREATE OR REPLACE AGGREGATE examples.average(int)
    SFUNC avg_state STYPE tuple<int, bigint> FINALFUNC avg_final
    INITCOND(0, 0);
    """
    future = session.execute(SimpleStatement(query)).wait()
    future.check()
    print("...create_functions")
}
func mrg(_ indent: Int = 0) -> String {
    return String(repeatElement(" ", count: indent))
}
func print_field_meta(meta: (name: String, value: Any?), indent: Int = 0) {
    if let val = meta.value {
        print("+++ \(mrg(indent))Field \"\(meta.name)\" value=\(val)")
    } else {
        print("+++ \(mrg(indent))Field \"\(meta.name)\" value=<null>")
    }
}
func print_column_meta(meta: ColumnMeta, indent: Int = 0) {
    print("+++ \(mrg(indent))Column=\(meta.name)")
    for fld in meta.fields {
        print_field_meta(meta: fld, indent: indent+1)
    }
}
func print_table_meta(meta: TableMeta, indent: Int = 0) {
    print("+++ ")
    print("+++ \(mrg(indent))Table \"\(meta.name)\"")
    for fld in meta.fields {
        print_field_meta(meta: fld, indent: indent+1)
    }
    for col in meta.columns {
        print_column_meta(meta: col, indent: indent+1)
    }
}
func print_function_meta(meta: FunctionMeta, indent: Int = 0) {
    print("+++ ")
    print("+++ \(mrg(indent))Function \"\(meta.name)\"")
    for fld in meta.fields {
        print_field_meta(meta: fld, indent: indent+1)
    }
}
func print_aggregate_meta(meta: AggregateMeta, indent: Int = 0) {
    print("+++ ")
    print("+++ \(mrg(indent))Aggregate \"\(meta.name)\"")
    for fld in meta.fields {
        print_field_meta(meta: fld, indent: indent+1)
    }
}
func print_keyspace_meta(meta: KeyspaceMeta, indent: Int = 0) {
    print("+++ ")
    print("+++ \(mrg(indent))Keyspace \"\(meta.name)\"")
    for fld in meta.fields {
        print_field_meta(meta: fld, indent: indent+1)
    }
    for tbl in meta.tables {
        print_table_meta(meta: tbl, indent: indent+1)
    }
    for fnc in meta.functions {
        print_function_meta(meta: fnc, indent: indent+1)
    }
    for agg in meta.aggregates {
        print_aggregate_meta(meta: agg, indent: indent+1)
    }
}
func print_keyspace(session: Session, keyspace: String) {
    if let ksp = session.schemaMeta.keyspaceMeta[keyspace] {
        print_keyspace_meta(meta: ksp,  indent: 0)
    }
}
func print_table(session: Session, keyspace: String, table: String) -> () {
    if let ksp = session.schemaMeta.keyspaceMeta[keyspace] {
        if let tbl = ksp.tableMeta[table] {
            print_table_meta(meta: tbl, indent: 0)
        } else {
            print("+++ table \"\(table)\" not found")
        }
    } else {
        print("+++ keyspace \"\(keyspace)\" not found")
    }
}
func print_function(session: Session, keyspace: String, function: String, arguments: String) -> () {
    print("+++ ")
    if let ksp = session.schemaMeta.keyspaceMeta[keyspace] {
        if let fnc = ksp.functionMeta[function, arguments] {
            print_function_meta(meta: fnc, indent: 0)
        } else {
            print("+++ function \"\(function)\" with arguments \"\(arguments)\" not found")
        }
    } else {
        print("+++ keyspace \"\(keyspace)\" not found")
    }
}
func print_aggregate(session: Session, keyspace: String, aggregate: String, arguments: String) -> () {
    print("+++ ")
    if let ksp = session.schemaMeta.keyspaceMeta[keyspace] {
        if let agg = ksp.aggregateMeta[aggregate, arguments] {
            print_aggregate_meta(meta: agg, indent: 0)
        } else {
            print("+++ aggregate \"\(aggregate)\" with arguments \"\(arguments)\" not found")
        }
    } else {
        print("+++ keyspace \"\(keyspace)\" not found")
    }
}
func schema_meta() {
    print("schema_meta...")
    let session = getSession()

    create_keyspace(session: session)
    create_table(session: session)
    create_functions(session: session)

    print_keyspace(session: session, keyspace: "examples")
    print_table(session: session, keyspace: "examples", table: "schema_meta");
    print_function(session: session, keyspace: "examples", function: "avg_state", arguments: "frozen<tuple<int, bigint>>, int");
    print_function(session: session, keyspace: "examples", function: "avg_final", arguments: "frozen<tuple<int, bigint>>");
    print_aggregate(session: session, keyspace: "examples", aggregate: "average", arguments: "int");

    print("...schema_meta")
}

