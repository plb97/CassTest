//
//  callbacks.swift
//  cpp-driver
//
//  Created by Philippe on 26/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Foundation
import Dispatch
import Cass

var semaphore: DispatchSemaphore = DispatchSemaphore(value: 0)
let checker = {(_ err_: Cass.Error?) -> Bool in
    if let err = err_?.error {
        print("*** CHECKER: Error=\(err)")
        semaphore.signal()
        return false
    }
    return true
}

func on_finish(_ future_: FutureBase?, _ data_: UnsafeMutableRawPointer? ) -> () {
    print("on_finish...")
    if !(future_?.check(checker: checker))! {
        return
    }
    semaphore.signal()
    print("...on_finish")
}

func on_select(_ future_: FutureBase?, _ data_: UnsafeMutableRawPointer?) -> () {
    print("on_select...")
    if !(future_?.check(checker: checker))! {
        return
    }
    if let future = future_ {
        let rs = future.result
        print("rows")
        for row in rs.rows() {
            let key = row.any(0) as! UUID
            let value = row.any(1) as! Date
            print("key=\(key) value=\(value)")
        }
        if nil != data_ {
            unowned let session = data_!.bindMemory(to: Session.self, capacity: 1).pointee
            let query = "USE examples;"
            session.execute(SimpleStatement(query), listener: Listener(on_finish,nil))
        }
    }
 print("...on_select")
}
func on_insert(_ future_: FutureBase?, _ data_: UnsafeMutableRawPointer?) -> () {
    print("on_insert...")
    if !(future_?.check(checker: checker))! {
        return
    }
    if nil != data_ {
        unowned let session = data_!.bindMemory(to: Session.self, capacity: 1).pointee
        let query = """
        SELECT key, value FROM examples.callbacks;
        """
        session.execute(SimpleStatement(query), listener: Listener(on_select,data_))
    }
    print("...on_insert")
}
func on_create_table(_ future_: FutureBase?, _ data_: UnsafeMutableRawPointer?) -> () {
    print("on_create_table...")
    if !(future_?.check(checker: checker))! {
        return
    }
    if let data = data_ {
        unowned let session = data.bindMemory(to: Session.self, capacity: 1).pointee
        let query = """
        INSERT INTO examples.callbacks (key, value)
        VALUES (?, ?);
        """
        let gen = UuidGen()
        let key = gen.time_uuid()
        let value = gen.timestamp(key)
        print("$$$ on_create_table: INSERT INTO key=\(key) value=\(value)")
        session.execute(SimpleStatement(query,key,value), listener: Listener(on_insert,data_))
    }
    print("...on_create_table")
}
func on_create_keyspace(_ future_: FutureBase?, _ data_: UnsafeMutableRawPointer?) -> () {
    print("on_create_keyspace...")
    if !(future_?.check(checker: checker))! {
        return
    }
    if nil != data_ {
        unowned let session = data_!.bindMemory(to: Session.self, capacity: 1).pointee
        let query = """
        CREATE TABLE IF NOT EXISTS examples.callbacks
        (key timeuuid PRIMARY KEY, value timestamp);
        """
        session.execute(SimpleStatement(query), listener: Listener(on_create_table,data_))
    }
    print("...on_create_keyspace")
}

func on_session_connect(_ future_: FutureBase?, _ data_: UnsafeMutableRawPointer? ) -> () {
    print("on_session_connect...")
    if !(future_?.check(checker: checker))! {
        return
    }
    if nil != data_ {
        unowned let session = data_!.bindMemory(to: Session.self, capacity: 1).pointee
        let query = """
        CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {
                               'class': 'SimpleStrategy', 'replication_factor': '3' };
        """
        session.execute(SimpleStatement(query), listener: Listener(on_create_keyspace,data_))
    }
    print("...on_session_connect")
}

func callbacks() {
    print("callbacks...")
    let session = Session()
    let data_ = UnsafeMutablePointer<Session>.allocate(capacity: 1)
    data_.initialize(to: session)
    defer {
        data_.deinitialize()
        data_.deallocate(capacity: 1)
    }
    Cluster()
        .setContactPoints("127.0.0.1")
        .setCredentials()
        .connect(session, listener: Listener(on_session_connect,data_))
    print("waiting")
    semaphore.wait()
    print("...callbacks")
}
