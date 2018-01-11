//
//  collections.swift
//  cpp-driver
//
//  Created by Philippe on 25/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

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
    CREATE TABLE IF NOT EXISTS examples.collections (key text,
                                                    items set<text>,
                                                    PRIMARY KEY (key));
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_table")
    future.check()
}
fileprivate
func insert_into(_ session: Session,_ key: String,_ items: Set<String>) -> () {
    print("insert_into_collections...")
    let query = "INSERT INTO examples.collections (key, items) VALUES (?, ?);"
    let statement = SimpleStatement(query,
                                    key,
                                    items)
    let future = session.execute(statement).wait()
    print("...insert_into_collections")
    future.check()
}
fileprivate
func select_from(_ session: Session,_ key: String) -> Result {
    print("select_from_collections...")
    let query = "SELECT key, items FROM examples.collections WHERE key = ?"
    let statement = SimpleStatement(query,key)
    let future = session.execute(statement).wait()
    future.check()
    print("...select_from_collections")
    return future.result
}

func collections() {
    print("collections...")

    let session = getSession()
    create_keyspace(session: session)
    create_table(session: session)

    let items: Set<String> = ["apple", "orange", "banana", "mango"]

    insert_into(session,KEY, items)
    let rs = select_from(session, KEY)
    /*print("first")
     if let row = rs.first() {
     let key = row.string(0)
     //print("row=",row.set_string(1))
     print("key=\(key) items=\(row.set_string(name: "items"))")
     }*/
    print("rows")
    for row in rs.rows {
        //let key = row.any(0) as! String
        //let items = row.any(1) as! Set<String>
        let key = row.any(0) as! String
        let items = row.any(1) as! Set<String>
        print("key=\(key) items=\(items)")
    }
    print("...collections")
}
