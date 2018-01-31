//
//  tuple.swift
//  CassTest
//
//  Created by Philippe on 13/01/2018.
//  Copyright © 2018 PLHB. All rights reserved.
//

import Cass

fileprivate let KEY = "basic_tuple"

fileprivate
func getSession() -> Session {
    let session = Session()
    session.connect(Cluster().setContactPoints("127.0.0.1").setCredentials())
        .wait()
        .check()
    return session
}

fileprivate
func create_keyspace(session: Session) {
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
func create_table(session: Session) {
    print("create_table...")
    let query = """
    CREATE TABLE IF NOT EXISTS examples.tuples (id timeuuid, item frozen<tuple<text, bigint>>, PRIMARY KEY(id));
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_table")
    future.check()
}
fileprivate
func insert_into(session: Session) {
    print("insert_into...")
    let query = "INSERT INTO examples.tuples (id, item) VALUES (?, ?);"
    let gen = UuidGen()
    let id = gen.time
    let id_str = id.string
    let id_time_and_version = Int64(id.time_and_version)
    print("$$$ insert_into: id_str=\(id_str) id_time_and_version=\(id_time_and_version)")
    let item = Tuple(count: 2)
    item[0] = id_str
    item[1] = id_time_and_version
    //let item = Tuple(id_str, id_time_and_version)
    let statement = SimpleStatement(query,id,item)
    let future = session.execute(statement).wait()
    print("...insert_into")
    future.check()
}
fileprivate
func select_from(session: Session) {
    print("select_from...")
    let query = "SELECT id, item FROM examples.tuples;"
    let statement = SimpleStatement(query)
    let future = session.execute(statement).wait()
    future.check()
    let rs = future.result
    print("rows")
    for row in rs.rows {
        let id = row.any(0) as! UUID
        let item = row.any(1) as! Tuple
        for item_value in item {
            print("id \(id) item_value \(item_value!)")
        }
    }
    print("...select_from")
}

func tuple() {
    print("tuple...")
    let session = getSession()
    defer {
        session.close().wait()
    }

    create_keyspace(session: session)
    create_table(session: session)
    insert_into(session: session)
    select_from(session: session)
    print("...tuple")
}

