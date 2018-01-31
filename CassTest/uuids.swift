//
//  uuids.swift
//  cpp-driver
//
//  Created by Philippe on 18/11/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Cass

fileprivate let KEY = "test"

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
    CREATE TABLE IF NOT EXISTS examples.log (key text, time timeuuid, entry text,
                                              PRIMARY KEY (key, time));
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_table")
    future.check()
}
fileprivate
func insert_into(session: Session, key: String, time: UUID, entry: String) {
    print("insert_into_log...")
    let query = "INSERT INTO examples.log (key, time, entry) VALUES (?, ?, ?);"
    let statement = SimpleStatement(query,
                                    key,
                                    time,
                                    entry)
    let future = session.execute(statement).wait()
    print("...insert_into_log")
    future.check()
}
fileprivate
func select_from(session: Session, key: String) -> Result {
    print("select_from_log...")
    let query = "SELECT key, time, entry FROM examples.log WHERE key = ?"
    //let statement = SimpleStatement(query, key)
    let map = ["key": key]
    let statement = SimpleStatement(query, map: map)
    let future = session.execute(statement).wait()
    future.check()
    print("...select_from_log")
    return future.result
}

func uuids() {
    print("uuids...")
    let session = getSession()
    defer {
        session.close().wait()
    }
    create_table(session: session)
    let gen = UuidGen()
    var uuid: UUID
    uuid = gen.time
    //print("*** uuid=\(uuid) \(string(uuid: uuid))")
    insert_into(session: session, key: KEY, time: uuid, entry: "Log entry #01")
    uuid = gen.time
    //print("*** uuid=\(uuid) \(string(uuid: uuid))")
    insert_into(session: session, key: KEY, time: uuid, entry: "Log entry #02")
    uuid = gen.time
    //print("*** uuid=\(uuid) \(string(uuid: uuid))")
    insert_into(session: session, key: KEY, time: uuid, entry: "Log entry #03")
    uuid = gen.time
    //print("*** uuid=\(uuid) \(string(uuid: uuid))")
    insert_into(session: session, key: KEY, time: uuid, entry: "Log entry #04")

    let rs = select_from(session: session, key: KEY)
    /*print("first")
     if let row = rs.first {
         let key = row.any(0) as! String
         let uuid = row.any(1) as! UUID
         let entry = row.any(2) as! String
         print("key=\(key) time=\(uuid.uuidString.lowercased()) entry=\(entry)")
     }*/
    print("rows")
    for row in rs.rows {
        let key = row.any(0) as! String
        let uuid = row.any(1) as! UUID
        let entry = row.any(2) as! String
        print("key=\(key) time=\(uuid.uuidString.lowercased()) entry=\(entry)")
    }
    print("...uuids")
}
