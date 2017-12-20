//
//  auth.swift
//  cpp-driver
//
//  Created by Philippe on 24/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Cass

fileprivate
func getSession() -> Session {
    let session = Session()
    _ = Cluster().setContactPoints("127.0.0.1,127.0.0.2,127.0.0.3").setCredentials().connect(session).check()
    return session
}

func auth() {
    print("auth...")
    //let cluster = BasicCluster("127.0.0.1,127.0.0.2,127.0.0.3"/*,username:"cassandra",password:"cassandra"*/)
    //print("cluster:\(cluster)")
    /*let credentials = ["cassandra", "cassandra"]
    let authenticator = Authenticator(
        initial_callback: on_auth_initial,
        challenge_callback: on_auth_challenge,
        success_callback: on_auth_success,
        cleanup_callback: on_auth_cleanup,
        data: credentials
    )
    */
    _ = getSession()
    print("Successfully connected!")

    print("...auth")
}
