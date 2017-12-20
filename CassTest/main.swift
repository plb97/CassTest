//
//  main.swift
//  CassTest
//
//  Created by Philippe on 19/12/2017.
//  Copyright © 2017 PLHB. All rights reserved.
//

//import Foundation
import Cass

print("*** start")
print("byte order \(NSHostByteOrder()) little=\(NS_LittleEndian) big=\(NS_BigEndian)")
//simple()
//basic()
auth()
//batch()
//bind_by_name()
//callbacks()
//collections()
//maps()
//uuids()
//date_time()
//decimal()

/*
 //let main = DispatchQueue.main
 let background = DispatchQueue.global()
 let ctr = 10

 print("*** doSyncWork")
 func doSyncWork() {
 background.sync { for _ in 1...ctr { print("Light") } }
 for _ in 1...ctr { print("Heavy") } } // main
 doSyncWork()

 print("*** doAsyncWork")
 func doAsyncWork() {
 background.async { for _ in 1...ctr { print("Light") } }
 for _ in 1...ctr { print("Heavy") } }                // main
 doAsyncWork()

 let asianWorker = DispatchQueue(label: "construction_worker_1")
 let brownWorker = DispatchQueue(label: "construction_worker_2")
 print("*** doLightWork")
 func doLightWork() {
 asianWorker.async { for _ in 1...ctr { print("👷") } }
 brownWorker.async { for _ in 1...ctr { print("👷🏽") } } }
 doLightWork()
 */
/*
 DispatchQueue.global().async {
 for i in 1...10 {
 print("async i=\(i)")
 }
 }
 DispatchQueue.global().sync {
 for i in 1...10 {
 print("sync i=\(i)")
 }
 }
 */
//fileprivate let semaphore = DispatchSemaphore(value:0)
//semaphore.signal()
//print("waiting")
//semaphore.wait()
print("*** end")
