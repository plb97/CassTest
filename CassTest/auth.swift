//
//  auth.swift
//  cpp-driver
//
//  Created by Philippe on 24/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Cass

fileprivate
struct Credentials: Response {
    let username: String
    let password: String
    let data_: UnsafeMutableRawPointer?
    init(username: String = "cassandra", password: String = "cassandra", data: UnsafeMutableRawPointer? = nil) {
        self.username = username
        self.password = password
        data_ = data
    }
    var response: Array<UInt8> {
        var resp = Array<UInt8>()
        resp.append(0)
        resp.append(contentsOf: username.utf8)
        resp.append(0)
        resp.append(contentsOf: password.utf8)
        return resp
    }
    var data: UnsafeMutableRawPointer? { return data_ }
}

fileprivate
func initialCallback(authenticator: Authenticator, response: Response) -> () {
    print("initialCallback...")
    print("*** inet=\(authenticator.address)")
    print("*** className=\(authenticator.className)")
    print("*** host=\(authenticator.hostname)")
    authenticator.setResponse(response: response.response)
    print("...initialCallback")
}
fileprivate
func challengeCallback(authenticator: Authenticator, response: Response, token: String?) -> () {
    print("challengeCallback...")
    print("token=\"\(token ?? "<nil>")\"")
    print("...challengeCallback")
}
fileprivate
func successCallback(authenticator: Authenticator, response: Response, token: String?) -> () {
    print("successCallback...")
    print("token=\"\(token ?? "<nil>")\"")
    print("...successCallback")
}
fileprivate
func cleanupCallback(authenticator: Authenticator, response: Response) -> () {
    print("cleanupCallback...")
    print("...cleanupCallback")
}
fileprivate
func dataCleanupCallback(resp: Response) -> () {
    print("dataCleanupCallback...")
    print("...dataCleanupCallback")
}
fileprivate
func getSession(authenticatorCallbacks: AuthenticatorCallbacks) -> Session {
    let session = Session()
//    _ = Cluster().setContactPoints("127.0.0.1,127.0.0.2,127.0.0.3")
    _ = Cluster().setContactPoints("127.0.0.1")
//        .setCredentials()
        .setAuthenticatorCallbacks(authenticatorCallbacks)
        .connect(session)
//        .wait()
        .check()
    return session
}
func auth() {
    print("auth...")
    let authenticatorCallbacks = AuthenticatorCallbacks(
        initialCallback: initialCallback,
        challengeCallback: challengeCallback,
        successCallback: successCallback,
        cleanupCallback: cleanupCallback,
        dataCleanupCallback: dataCleanupCallback, 
        response: Credentials())
    let session = getSession(authenticatorCallbacks: authenticatorCallbacks)
    defer {
       _ = session.close().wait()
    }
    print("Successfully connected!")

    print("...auth")
}
