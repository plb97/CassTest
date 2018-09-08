//
//  simple.swift
//  cpp-driver
//
//  Created by Philippe on 21/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Cass

fileprivate
func getSession() -> Session {
    let session = Session()
    session.connect(Cluster()
        .setContactPoints(HOSTS)
        .setCredentials()
        )
        .wait().check()
    return session
}

fileprivate
func select_from(session: Session) -> Result {
    let query = "SELECT release_version FROM system.local;"
    let future = session.execute(SimpleStatement(query)).wait()
    future.check()
    return future.result
}

func simple() {
    print("simple...")
    let session = getSession()
    defer {
        session.close().wait()
    }
    let rs = select_from(session: session)
    if let row = rs.first {
        print("select")
        let release_version = row.any(0) as! String
        //let release_version = row.any(name:"release_version") as! String
        print("release_version: \(release_version)")
    } else {
        fatalError("select error")
    }
    print("...simple")
}
