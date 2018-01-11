//
//  callbacks.swift
//  cpp-driver
//
//  Created by Philippe on 26/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Dispatch
import Cass

fileprivate var semaphore: DispatchSemaphore = DispatchSemaphore(value: 0)
fileprivate let checker = {(_ err: Cass.Error) -> Bool in
    if !err.ok {
        print("*** CHECKER: Error=\(err)")
        semaphore.signal()
        return false
    }
    return true
}

fileprivate func on_finish(_ parm: Listener_t) -> () {
    print("on_finish...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    semaphore.signal()
    print("...on_finish")
}

fileprivate func on_select(_ parm: Listener_t) -> () {
    print("on_select...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    if let data = parm.dataPointer {
        let session = data.bindMemory(to: Session.self, capacity: 1).pointee
        let listener = Listener(callback: on_finish, data: session)
        let rs = parm.future.result
        print("rows")
        for row in rs.rows {
            let key = row.any(0) as! UUID
            let value = row.any(1) as! Date
            print("key=\(key) value=\(value)")
        }
        let query = "USE examples;"
        session.execute(SimpleStatement(query), listener: listener)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_select")
}

fileprivate func on_insert(_ parm: Listener_t) -> () {
    print("on_insert...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    if let data = parm.dataPointer {
        let query = """
        SELECT key, value FROM examples.callbacks;
        """
        let session = data.bindMemory(to: Session.self, capacity: 1).pointee
        let listener = Listener(callback: on_select, data: session)
        session.execute(SimpleStatement(query), listener: listener)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_insert")
}


fileprivate func on_create_table(_ parm: Listener_t) -> () {
    print("on_create_table...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    if let data = parm.dataPointer {
        let query = """
        INSERT INTO examples.callbacks (key, value)
        VALUES (?, ?);
        """
        let session = data.bindMemory(to: Session.self, capacity: 1).pointee
        let listener = Listener(callback: on_insert, data: session)
        let gen = UuidGen()
        let key = gen.time_uuid()
        let value = gen.timestamp(key)
        print("$$$ on_create_table: \(query) key=\(key) value=\(value)")
        session.execute(SimpleStatement(query,key,value), listener: listener)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_create_table")
}

fileprivate func on_create_keyspace(_ parm: Listener_t) -> () {
    print("on_create_keyspace...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    if let data = parm.dataPointer {
        let query = """
        CREATE TABLE IF NOT EXISTS examples.callbacks
        (key timeuuid PRIMARY KEY, value timestamp);
        """
        let session = data.bindMemory(to: Session.self, capacity: 1).pointee
        let listener = Listener(callback: on_create_table, data: session)
        session.execute(SimpleStatement(query), listener: listener)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_create_keyspace")
}

fileprivate func on_session_connect(_ parm: Listener_t) -> () {
    print("on_session_connect...")
    if !(parm.future.check(checker: checker)) {
        print("*** \(parm.future.errorMessage)")
        return
    }
    if let data = parm.dataPointer {
        let query = """
        CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {
                               'class': 'SimpleStrategy', 'replication_factor': '3' };
        """
        let session = data.bindMemory(to: Session.self, capacity: 1).pointee
        let listener = Listener(callback: on_create_keyspace, data: session)
        session.execute(SimpleStatement(query), listener: listener)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_session_connect")
}

func callbacks() {
    print("callbacks...")
    let session = Session()
    let listener = Listener(callback: on_session_connect, data: session)
    session.connect(Cluster().setContactPoints("127.0.0.1").setCredentials(), listener: listener)
    print("waiting")
    semaphore.wait()
    print("...callbacks")
}
