//
//  callbacks.swift
//  cpp-driver
//
//  Created by Philippe on 26/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Cass
import Dispatch

fileprivate var group: DispatchGroup = DispatchGroup()

fileprivate func on_finish(_ parm: CallbackData) {
    defer {
        parm.callback.deallocData(as: Session.self)
    }
    print("on_finish...")
    if !(parm.future.setChecker(okPrintChecker).check()) {
        print("*** \(parm.future.errorMessage)")
        defer {
            group.leave()
        }
        return
    }
    group.leave()
    print("...on_finish")
}

fileprivate func on_select(_ parm: CallbackData) {
    defer {
        parm.callback.deallocData(as: Session.self)
    }
    let query = "USE examples;"
    print("on_select...")
    if !(parm.future.setChecker(okPrintChecker).check()) {
        print("*** \(parm.future.errorMessage)")
        defer {
            group.leave()
        }
        return
    }
    let rs = parm.future.result
    print("rows")
    for row in rs.rows {
        let key = row.any(0) as! UUID
        let value = row.any(1) as! Date
        print("key=\(key) value=\(value)")
    }
    if let session = parm.callback.data(as: Session.self) {
        let callback = Callback(callback: on_finish, data: session)
        session.execute(SimpleStatement(query), callback: callback)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_select")
}

fileprivate func on_insert(_ parm: CallbackData) {
    defer {
        parm.callback.deallocData(as: Session.self)
    }
    print("on_insert...")
    if !(parm.future.setChecker(okPrintChecker).check()) {
        print("*** \(parm.future.errorMessage)")
        defer {
            group.leave()
        }
        return
    }
    let query = """
        SELECT key, value FROM examples.callbacks;
        """
    if let session = parm.callback.data(as: Session.self) {
        let callback = Callback(callback: on_select, data: session)
        session.execute(SimpleStatement(query), callback: callback)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_insert")
}

fileprivate func on_create_table(_ parm: CallbackData) {
    defer {
        parm.callback.deallocData(as: Session.self)
    }
    print("on_create_table...")
    if !(parm.future.setChecker(okPrintChecker).check()) {
        print("*** \(parm.future.errorMessage)")
        defer {
            group.leave()
        }
        return
    }
    let query = """
        INSERT INTO examples.callbacks (key, value)
        VALUES (?, ?);
        """
    if let session = parm.callback.data(as: Session.self) {
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

fileprivate func on_create_keyspace(_ parm: CallbackData) {
    defer {
        parm.callback.deallocData(as: Session.self)
    }
    print("on_create_keyspace...")
    if !(parm.future.setChecker(okPrintChecker).check()) {
        print("*** \(parm.future.errorMessage)")
        defer {
            group.leave()
        }
        return
    }
    let query = """
        CREATE TABLE IF NOT EXISTS examples.callbacks
        (key timeuuid PRIMARY KEY, value timestamp);
        """
    if let session = parm.callback.data(as: Session.self) {
        let callback = Callback(callback: on_create_table, data: session)
        session.execute(SimpleStatement(query), callback: callback)
    } else {
        fatalError("Ne devrait pas arriver")
    }
    print("...on_create_keyspace")
}

fileprivate func on_session_connect(_ parm: CallbackData) {
    defer {
        parm.callback.deallocData(as: Session.self)
    }
    print("on_session_connect...")
    if !(parm.future.setChecker(okPrintChecker).check()) {
        print("*** \(parm.future.errorMessage)")
        defer {
            group.leave()
        }
        return
    }
    let query = """
        CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {
                               'class': 'SimpleStrategy', 'replication_factor': '3' };
        """
    if let session = parm.callback.data(as: Session.self) {
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
    group.enter()
    session.connect(Cluster().setContactPoints(HOSTS).setCredentials(), callback: callback)
    print("waiting")
    group.wait()
    print("...callbacks")
}
