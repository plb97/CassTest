//
//  date_time.swift
//  cpp-driver
//
//  Created by Philippe on 20/11/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Cass

fileprivate let KEY = "date_time_test"

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
    let future = session.execute(SimpleStatement(query))
    print("...create_keyspace")
    future.check()
}
fileprivate
func create_table(session: Session) {
    print("create_table...")
    let query = """
    CREATE TABLE IF NOT EXISTS examples.timestamp (key text PRIMARY KEY,  dt timestamp);
    """
    let future = session.execute(SimpleStatement(query))
    print("...create_table")
    future.check()
}
fileprivate
func insert_into(session: Session, key: String, timestamp: Date) {
    print("insert_into...")
    let query = "INSERT INTO examples.timestamp (key, dt) VALUES (?, ?);"
    let statement = SimpleStatement(query,
                                    key,
                                    timestamp)
    let future = session.execute(statement)
    print("...insert_into")
    future.check()
}
fileprivate
func select_from(session: Session, key: String) -> Result {
    print("select_from...")
    let query = "SELECT key, dt FROM examples.timestamp WHERE key = ?;"
    //let statement = SimpleStatement(query, key)
    let map = ["key": key]
    let statement = SimpleStatement(query, map: map)
    let future = session.execute(statement)
    future.wait().check()
    print("...select_from")
    return future.result
}

func date_time() {
    print("date_time...")
    let session = getSession()
    defer {
        session.close().wait()
    }
    create_keyspace(session: session)
    create_table(session: session)
    let now = Date()
    print("*** now=\(now)")
    insert_into(session: session, key: KEY, timestamp: now)
    let rs = select_from(session: session, key: KEY)
    //let locale = Locale(identifier: "fr_FR")
    //let df = DateFormatter()
    //df.dateFormat = "yyyy-MM-dd'T'HH:mm:ssZZZZZ"
    //df.dateStyle = .long
    //df.timeStyle = .long
    /*print("first")
     if let row = rs.first() {
         let key = row.any(0) as! String
         let dt = row.any(1) as! Date
         //print("key=\(key) dt=\(dt.description(with: locale))")
         //print("key=\(key) dt=\(df.string(from: dt))")
         print("key=\(key) dt=\(dt)")
     }*/
    print("rows")
    for row in rs.rows {
        let key = row.any(0) as! String
        let dt = row.any(1) as! Date
        //print("key=\(key) dt=\(dt.description(with: locale))")
        //print("key=\(key) dt=\(df.string(from: dt))")
        print("key=\(key) dt=\(dt)")
    }
    print("...date_time")
}
