//
//  decimal.swift
//  cpp-driver
//
//  Created by Philippe on 20/11/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Foundation
import Cass

fileprivate
let KEY = "test_decimal"

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
    let future = session.execute(SimpleStatement(query))
    print("...create_keyspace")
    future.check()
}
fileprivate
func create_table(session: Session) -> () {
    print("create_table...")
    let query = """
    CREATE TABLE IF NOT EXISTS examples.decimal (key text PRIMARY KEY,  d decimal);
    """
    let future = session.execute(SimpleStatement(query))
    print("...create_table")
    future.check()
}
fileprivate
func insert_into(session: Session, key: String, decimal: Decimal) -> () {
    print("insert_into...")
    let query = "INSERT INTO examples.decimal (key, d) VALUES (?, ?);"
    let statement = SimpleStatement(query,
                                    key,
                                    decimal)
    let future = session.execute(statement)
    print("...insert_into")
    future.check()
}
fileprivate
func select_from(session: Session, key: String) -> Result {
    print("select_from...")
    let query = "SELECT key, d FROM examples.decimal;"
    //let query = "SELECT key, d FROM examples.decimal WHERE key = ?;"
    //let statement = SimpleStatement(query, key)
    //let map = ["key": key]
    //let statement = SimpleStatement(query, map: map)
    let statement = SimpleStatement(query)
    let future = session.execute(statement)
    future.wait().check()
    print("...select_from")
    return future.result
}

func decimal() {
    print("decimal...")
    let session = getSession()
    create_keyspace(session: session)
    create_table(session: session)
    let dec = Decimal(56.78)
    let (varint, varint_size, int32) = dec.cass
    print("*** dec=\(dec) \(type(of:dec)) varint=\(varint) varint_size=\(varint_size) int32=\(int32)")
    insert_into(session: session, key: KEY, decimal: dec)
    let rs = select_from(session: session, key: KEY)
    /*print("first")
     if let row = rs.first {
         print("row=\(row)")
         let key = row.any(0) as! String
         if let d = row.any(1) as? Decimal {
         print("key=\(key) d=\(d)")
         }
     }*/
    print("rows")
    for row in rs.rows {
        print("row=\(row)")
        let key = row.any(0) as! String
        let d = row.any(1) as! Decimal
        print("key=\(key) d=\(d)")
    }
    print("...decimal")
}
