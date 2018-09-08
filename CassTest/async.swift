//
//  async.swift
//  CassTest
//
//  Created by Philippe on 29/12/2017.
//  Copyright © 2017 PLHB. All rights reserved.
//

import Cass

fileprivate let KEY = "async_test"
fileprivate let NUM_CONCURRENT_REQUESTS = 1_000

fileprivate
func getSession() -> Session {
    let session = Session()
    session.connect(Cluster().setContactPoints(HOSTS).setCredentials()).wait().check()
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
    CREATE TABLE IF NOT EXISTS examples.async (key text,
                                  bln boolean,
                                  flt float, dbl double,
                                  i32 int, i64 bigint,
                                  PRIMARY KEY (key));
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_table")
    future.check()
}
fileprivate
func insert_into(session: Session, key: String) {
    print("insert_into...")
    let query = "INSERT INTO examples.async (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);"
    var futures = Array<Future>()
    for i in 0 ..< NUM_CONCURRENT_REQUESTS {
        let key_buffer = String(format:"%@_%03d",key,i) //"\(key)_\(i)"
        //print("key_buffer=\(key_buffer)")
        let statement = SimpleStatement(query
                                        ,key_buffer // key
                                        ,0 == i % 2 // bln
                                        ,Float(i) / 2 // flt
                                        ,Double(i) / 200 // dbl
                                        ,Int32(i) * 10 // i32
                                        ,Int64(i) * 100 // i64
        )
        futures.append(session.execute(statement).setChecker(okPrintChecker))
    }
    for i in 0 ..< NUM_CONCURRENT_REQUESTS {
        let future = futures[i]
        if !future.wait().check() {
            print("*** \(future.errorMessage)")
        }
    }
    print("...insert_into")
}

func async() {
    print("async...")

    let session = getSession()
    defer {
        session.close().wait()
    }
    create_keyspace(session: session)
    create_table(session: session)
    insert_into(session: session, key: KEY)

    print("...async")
}
