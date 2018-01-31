//
//  callbacks.swift
//  cpp-driver
//
//  Created by Philippe on 26/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Cass
import Dispatch

fileprivate var semaphore: DispatchSemaphore = DispatchSemaphore(value: 0)
fileprivate let checker = {(_ err: Cass.Error) -> Bool in
    if .ok != err {
        print("*** CHECKER: Error=\(err)")
        semaphore.signal()
        return false
    }
    return true
}

fileprivate func on_finish(_ parm: CallbackData) -> () {
    defer {
        parm.dealloc(Session.self)
    }
    print("on_finish...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    semaphore.signal()
    print("...on_finish")
}

fileprivate func on_select(_ parm: CallbackData) -> () {
    defer {
        parm.dealloc(Session.self)
    }
    let query = "USE examples;"
    print("on_select...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    let rs = parm.future.result
    print("rows")
    for row in rs.rows {
        let key = row.any(0) as! UUID
        let value = row.any(1) as! Date
        print("key=\(key) value=\(value)")
    }
    if let session = parm.data(as: Session.self) {
        let callback = Callback(callback: on_finish, data: session)
        session.execute(SimpleStatement(query), callback: callback)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_select")
}

fileprivate func on_insert(_ parm: CallbackData) -> () {
    defer {
        parm.dealloc(Session.self)
    }
    let query = """
        SELECT key, value FROM examples.callbacks;
        """
    print("on_insert...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    if let session = parm.data(as: Session.self) {
        let callback = Callback(callback: on_select, data: session)
        session.execute(SimpleStatement(query), callback: callback)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_insert")
}

fileprivate func on_create_table(_ parm: CallbackData) -> () {
    defer {
        parm.dealloc(Session.self)
    }
    let query = """
        INSERT INTO examples.callbacks (key, value)
        VALUES (?, ?);
        """
    print("on_create_table...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    if let session = parm.data(as: Session.self) {
        let callback = Callback(callback: on_insert, data: session)
        let gen = UuidGen()
        let key: UUID = gen.time
        let value = Date(timestamp: Int64(key.timestamp))
        print("$$$ on_create_table: \(query) key=\(key) value=\(value)")
        session.execute(SimpleStatement(query,key,value), callback: callback)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_create_table")
}

fileprivate func on_create_keyspace(_ parm: CallbackData) -> () {
    defer {
        parm.dealloc(Session.self)
    }
    let query = """
        CREATE TABLE IF NOT EXISTS examples.callbacks
        (key timeuuid PRIMARY KEY, value timestamp);
        """
    print("on_create_keyspace...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    if let session = parm.data(as: Session.self) {
        let callback = Callback(callback: on_create_table, data: session)
        session.execute(SimpleStatement(query), callback: callback)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_create_keyspace")
}

fileprivate func on_session_connect(_ parm: CallbackData) -> () {
    defer {
        parm.dealloc(Session.self)
    }
    let query = """
        CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {
                               'class': 'SimpleStrategy', 'replication_factor': '3' };
        """
    print("on_session_connect...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    if let session = parm.data(as: Session.self) {
        let callback = Callback(callback: on_create_keyspace, data: session)
        session.execute(SimpleStatement(query), callback: callback)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_session_connect")
}

func callbacks() {
    print("callbacks...")
    let session = Session()
    defer {
        session.close().wait()
    }
    let callback = Callback(callback: on_session_connect, data: session)
    session.connect(Cluster().setContactPoints("127.0.0.1").setCredentials(), callback: callback)
    print("waiting")
    semaphore.wait()
    //print("count=\(CFGetRetainCount(session))")
    print("...callbacks")
}
