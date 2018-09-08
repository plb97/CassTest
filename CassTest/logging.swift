//
//  logging.swift
//  CassTest
//
//  Created by Philippe on 26/01/2018.
//  Copyright © 2018 PLHB. All rights reserved.
//

import Cass
import Foundation

fileprivate
func getSession() -> Session {
    let session = Session()
    session.connect(Cluster().setContactPoints(HOSTS).setCredentials()).wait().check()
    return session
}
fileprivate
func on_log(_ parm: LogCallbackData) {
    if let log_file = parm.callback.data(as: FileHandle.self) {
        print("on_log: \(parm.logMessage.severity) \(parm.logMessage.message)")
        log_file.write(Data((parm.logMessage.description+"\n").utf8))
    } else {
        fatalError()
    }
}
func logging() {
    print("logging...")
    LogMessage.setLevel(.debug)
    let file = "driver.log"
    let fileManager = FileManager()
    print("jounal: \(fileManager.currentDirectoryPath)/\(file)")
    if !fileManager.fileExists(atPath: file) {
        if !fileManager.createFile(atPath: file, contents: nil) {
            print("Impossible de creer le fichier \(file)")
            fatalError("Impossible de creer le fichier \(file)")
        }
    }
    if let log_file = FileHandle(forWritingAtPath: file) {
        let logCallback = LogCallback(function: on_log, data: log_file)
        let session = getSession()
        defer {
            session.close().wait()
            log_file.synchronizeFile()
            log_file.closeFile()
            logCallback.deallocData(as: FileHandle.self)
        }
    } else {
        print("Impossible d'ouvrir le fichier \(file)")
        fatalError("Impossible d'ouvrir le fichier \(file)")
    }
    print("...logging")
}
