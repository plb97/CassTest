//
//  batch.swift
//  cpp-driver
//
//  Created by Philippe on 25/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Cass

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
    CREATE TABLE IF NOT EXISTS examples.pairs (key text,
                                              value text,
                                              PRIMARY KEY (key));
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_table")
    future.check()
}
fileprivate
func insert_into(session: Session,_ pairs: [[String]]) -> () {
    print("insert_into...")
    let batch = Batch(.logged)
    let query = "INSERT INTO examples.pairs (key, value) VALUES (?, ?);"

    let prepare = session.prepare(query).wait()
    prepare.check()
    let prepared = prepare.prepared
    //    let map = ["key": key]
    for pair in pairs {
        batch.addStatement(prepared.statement.bind(pair[0],pair[1])).check()
    }
//    for pair in pairs {
//        batch.addStatement(SimpleStatement(query, pair[0],pair[1])).check()
//    }
    batch.addStatement(SimpleStatement("INSERT INTO examples.pairs (key, value) VALUES ('c', '3');")).check()
    batch.addStatement(SimpleStatement(query,"d","4")).check()
    let future = session.execute(batch: batch).wait()
    print("...insert_into")
    future.check()
}

func batch() {
    print("batch...")
    
    let pairs = [["a", "1"], ["b", "2"]]

    let session = getSession()
    create_keyspace(session: session)
    create_table(session: session)
    insert_into(session: session, pairs)
    print("...batch")
}
