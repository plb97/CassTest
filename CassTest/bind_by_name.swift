//
//  bind_by_name.swift
//  cpp-driver
//
//  Created by Philippe on 25/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Cass

fileprivate let KEY = "bind_by_name_test"

fileprivate
struct Basic {
    var bln: Bool
    var flt: Float
    var dbl: Double
    var i32: Int32
    var i64: Int64
}

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
    CREATE TABLE IF NOT EXISTS examples.basic (key text,
                                              bln boolean,
                                              flt float, dbl double,
                                              i32 int, i64 bigint,
                                              PRIMARY KEY (key));
    """
    let future = session.execute(SimpleStatement(query))
    print("...create_table")
    future.check()
}
fileprivate
func insert_into(session: Session, key: String, basic: Basic) {
    print("insert_into...")
    let query = "INSERT INTO examples.basic (key, bln, flt, dbl, i32, i64) VALUES (?, ?, ?, ?, ?, ?);"
    let statement = SimpleStatement(query,
                                    key,
                                    basic.bln,
                                    basic.flt,
                                    basic.dbl,
                                    basic.i32,
                                    basic.i64)
    /*let map: [String: Any?] = [
     "key": key,
     "bln": basic.bln,
     "flt": basic.flt,
     "dbl": basic.dbl,
     "i32": basic.i32,
     "i64": basic.i64,
     ]
     let statement = SimpleStatement(query, map: map)*/
    let future = session.execute(statement)
    print("...insert_into")
    future.check()
}
fileprivate
func select_from(session: Session, key: String) -> Result {
    print("select_from...")
    let query = "SELECT key, bln, flt, dbl, i32, i64 FROM examples.basic WHERE key = ?"
    //let statement = SimpleStatement(query, key)
    let statement = SimpleStatement(query, map: ["key": key])
    let future = session.execute(statement)
    future.wait().check()
    print("...select_from")
    return future.result
}

func bind_by_name() {
    print("bind_by_name...")
    let session = getSession()
    defer {
        session.close().wait()
    }
    create_keyspace(session: session)
    create_table(session: session)
    let basic = Basic(bln: true, flt: 0.001, dbl: 0.0002, i32: 3, i64: 4)
    print("basic",basic)
    insert_into(session: session, key: KEY, basic: basic)
    let rs = select_from(session: session, key: KEY)
    /*print("first")
     if let row = rs.first() {
         //print("key=",row.any(0) as! String)
         //let basic = Basic(bln: row.any(1) as! Bool,
         //                  flt: row.any(2) as! Float,
         //                  dbl: row.any(3) as! Double,
         //                  i32: row.any(4) as! Int32,
         //                  i64: row.any(5) as! Int64)
         //print("basic=",basic)
         print("string key",row.any(name: "key") as! String)
         print("bool BLN",row.any(name: "BLN") as! Bool)
         print("float flt",row.any(name: "flt") as! Float)
         print("double dbl",row.any(name: "dbl") as! Double)
         print("int32 \"i32\"",row.any(name: "\"i32\"") as! Int32)
         print("int64 i64",row.any(name: "i64") as! Int64)
     }*/
    print("*** rows ***")
    for row in rs.rows {
        //print("key=",row.any(0) as! String)
        //let basic = Basic(bln: row.any(1) as! Bool,
        //                  flt: row.any(2) as! Float,
        //                  dbl: row.any(3) as! Double,
        //                  i32: row.any(4) as! Int32,
        //                  i64: row.any(5) as! Int64)
        //print("basic=",basic)
        print("string key",row.any(name: "key") as! String)
        print("bool BLN",row.any(name: "BLN") as! Bool)
        print("float flt",row.any(name: "flt") as! Float)
        print("double dbl",row.any(name: "dbl") as! Double)
        print("int32 \"i32\"",row.any(name: "\"i32\"") as! Int32)
        print("int64 i64",row.any(name: "i64") as! Int64)
    }
    print("...bind_by_name")
}
