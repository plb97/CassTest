//
//  duration.swift
//  CassTest
//
//  Created by Philippe on 29/12/2017.
//  Copyright © 2017 PLHB. All rights reserved.
//

import Cass

fileprivate let NANOS_IN_A_SEC: Int64 = 1_000_000_000

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
    CREATE TABLE IF NOT EXISTS examples.duration
    (key text PRIMARY KEY, d duration);
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_table")
    future.check()
}
fileprivate
func insert_into(session: Session, key: String, months: Int32, days: Int32, nanos: Int64) -> () {
    print("insert_into...")
    let query = "INSERT INTO examples.duration (key, d) VALUES (?, ?);"
    let statement = SimpleStatement(query,
                                    key,
                                    Duration(months: months, days: days, nanos: nanos))
    let future = session.execute(statement).wait()
    print("...insert_into")
    future.check()
}
fileprivate
func select_from(session: Session, key: String) -> Result {
    print("select_from...")
    let query = "SELECT key, d FROM examples.duration WHERE key = ?;"
    let map = ["key": key]
    let statement = SimpleStatement(query, map: map)
    let future = session.execute(statement)
    future.wait().check()
    print("...select_from")
    return future.result
}

func duration() {
    print("duration...")
    let session = getSession()
    defer {
        session.close().wait()
    }
    create_keyspace(session: session)
    create_table(session: session)

    insert_into(session: session, key: "zero", months: 0, days: 0, nanos: 0)
    insert_into(session: session, key: "one_month_two_days_three_seconds", months: 1, days: 2, nanos: 3 * NANOS_IN_A_SEC)
    insert_into(session: session, key: "negative_one_month_two_days_three_seconds", months: -1, days: -2, nanos: -3 * NANOS_IN_A_SEC)

    var rs: Result
    print("**** rows")
    rs = select_from(session: session, key: "zero");
    for row in rs.rows {
        let key = row.any(0) as! String
        let d = row.any(1) as! Duration
        print("*** key=\(key) months=\(d.months) days=\(d.days) nanos=\(d.nanos)")
    }
    print("**** rows")
    rs = select_from(session: session, key: "one_month_two_days_three_seconds");
    for row in rs.rows {
        let key = row.any(0) as! String
        let d = row.any(1) as! Duration
        print("*** key=\(key) months=\(d.months) days=\(d.days) nanos=\(d.nanos)")
    }
    print("**** rows")
    rs = select_from(session: session, key: "negative_one_month_two_days_three_seconds");
    for row in rs.rows {
        let key = row.any(0) as! String
        let d = row.any(1) as! Duration
        print("*** key=\(key) months=\(d.months) days=\(d.days) nanos=\(d.nanos)")
    }

    print("...duration")
}
