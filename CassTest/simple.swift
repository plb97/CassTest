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
    _ = Cluster().setContactPoints("127.0.0.1").setCredentials().connect(session).check()
    return session
}

fileprivate
func select_from(session: Session) -> Result {
    let query = "SELECT release_version FROM system.local"
    let rs = session.execute(SimpleStatement(query)).result
    _ = rs.check()
    return rs
}

func simple() {
    print("simple...")
    let session = getSession()
    let rs = select_from(session: session)
    if let row = rs.first() {
        print("select")
        //let release_version = row.any(0) as! String
        let release_version = row.any(name:"release_version") as! String
        print("release_version: \(release_version)")
    } else {
        fatalError("select error")
    }
    print("...simple")
}
