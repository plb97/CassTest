//
//  paging.swift
//  CassTest
//
//  Created by Philippe on 31/12/2017.
//  Copyright © 2017 PLHB. All rights reserved.
//

import Cass

fileprivate let NUM_CONCURRENT_REQUESTS = 17
fileprivate let PAGING_SIZE: Int32 = 7

fileprivate
func getSession() -> Session {
    let session = Session()
    session.connect(Cluster().setContactPoints("127.0.0.1").setCredentials()).wait().check()
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
    CREATE TABLE IF NOT EXISTS examples.paging (key timeuuid,
                                   value text,
                                   PRIMARY KEY (key));
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_table")
    future.check()
}
fileprivate
func insert_into(session: Session) {
    print("insert_into...")
    let gen = UuidGen()
    let query = "INSERT INTO examples.paging (key, value) VALUES (?, ?);"
    var futures = Array<Future>()
    for i in 0 ..< NUM_CONCURRENT_REQUESTS {
        let key = gen.time
        let value = String(format:"%03d",i)
//        print("insert_into: key: \(key) value: '\(value)'")
        let statement = SimpleStatement(query, key, value)
        futures.append(session.execute(statement).setChecker(okPrintChecker))
    }
    for i in 0 ..< NUM_CONCURRENT_REQUESTS {
        let future = futures[i]
        future.wait().check()
    }
    print("...insert_into")
}
fileprivate
func select_from(session: Session) {
    print("select_from...")
    let query = "SELECT key, value FROM examples.paging;"
    let statement = SimpleStatement(query).setPagingSize(PAGING_SIZE)
    var cond = false
    repeat {
        print("---")
        let future = session.execute(statement)
        if !future.check() {
            break
        }
        let rs = future.result
        for row in rs.rows {
            let key = row.any(0) as! UUID
            let value = row.any(1) as! String
            print("key: \(key) value:'\(value)'")
        }
        cond = statement.hasMorePages(result: rs)
    } while cond
    print("...select_from")
}

func paging() {
    print("paging...")

    let session = getSession()
    defer {
        session.close().wait()
    }
    create_keyspace(session: session)
    create_table(session: session)
    insert_into(session: session)
    select_from(session: session)

    print("...paging")
}

