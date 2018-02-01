//
//  perf.swift
//  CassTest
//
//  Created by Philippe on 31/01/2018.
//  Copyright © 2018 PLHB. All rights reserved.
//

import Cass
import Dispatch

fileprivate let NUM_THREADS = 1
fileprivate let NUM_IO_WORKER_THREADS = 4
fileprivate let QUEUE_SIZE_IO = 10000
fileprivate let NUM_CONCURRENT_REQUESTS = 1000//0
fileprivate let NUM_ITERATIONS = 100//0
fileprivate let DELTA_SECS = 5

fileprivate let DO_SELECTS = true
fileprivate let USE_PREPARED = true

fileprivate let select_query = "SELECT id, title, album, artist, tags FROM stress.songs WHERE id = a98d21b2-1900-11e4-b97b-e5e358e71e0d;"
fileprivate let insert_query = "INSERT INTO stress.songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?);"

fileprivate let tags = Set<String>(arrayLiteral: "jazz", "2013")
fileprivate let big_string = String(repeating: "0123456701234567012345670123456701234567012345670123456701234567", count: 3)

fileprivate var queues = [DispatchQueue]()

fileprivate let checker = {(_ err: Cass.Error) -> Bool in
    if .ok != err {
        //print("*** CHECKER: Error=\(err)")
        return false
    }
    return true
}

fileprivate
func getSession() -> Session {
    let session = Session()
    session.connect(Cluster()
        .setContactPoints("127.0.0.1")
        .setCredentials()
        .setNumThreadsIo(NUM_IO_WORKER_THREADS)
        .setQueueSizeIo(QUEUE_SIZE_IO)
        .setCoreConnectionsPerHost(1)
        .setMaxConnectionsPerHost(2)
        ).wait().check()
    return session
}

fileprivate
func drop_keyspace(session: Session) {
    print("drop_keyspace...")
    let query = """
    DROP KEYSPACE IF EXISTS stress;
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...drop_keyspace")
    future.check()
}
fileprivate
func create_keyspace(session: Session) {
    print("create_keyspace...")
    let query = """
    CREATE KEYSPACE IF NOT EXISTS stress WITH replication = {
                           'class': 'SimpleStrategy', 'replication_factor': '3' };
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_keyspace")
    future.check()
}
fileprivate
func create_table(session: Session) {
    print("create_table...")
    let query = """
    CREATE TABLE IF NOT EXISTS stress.songs (id uuid PRIMARY KEY,
                         title text, album text, artist text,
                         tags set<text>, data blob);
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_table")
    future.check()
}
fileprivate func insert_into_perf(session: Session) {
    //print("insert_into_perf")
    let gen = UuidGen()
    var futures = Array<Future>()
    for _ in 0 ..< NUM_CONCURRENT_REQUESTS {
        let id = gen.time
        let statement = SimpleStatement(insert_query
            ,id
            ,big_string
            ,big_string
            ,big_string
            ,tags
        ).setIsIdempotent(true)
        futures.append(session.execute(statement))
    }
    for i in 0 ..< NUM_CONCURRENT_REQUESTS {
        let future = futures[i]
        if !future.wait().check(checker: checker) {
            //print("*** \(future.errorMessage)")
        }
    }
}
fileprivate func insert_into_perf_prepared(session: Session) {
    //print("insert_into_perf_prepared")
    let prepare = session.prepare(insert_query).wait()
    prepare.check()
    let prepared = prepare.prepared
    let gen = UuidGen()
    var futures = Array<Future>()
    for _ in 0 ..< NUM_CONCURRENT_REQUESTS {
        let id = gen.time
        let statement = prepared.statement.bind(id
            ,big_string
            ,big_string
            ,big_string
            ,tags
        ).setIsIdempotent(true)
        futures.append(session.execute(statement))
    }
    for i in 0 ..< NUM_CONCURRENT_REQUESTS {
        let future = futures[i]
        if !future.wait().check(checker: checker) {
            //print("*** \(future.errorMessage)")
        }
    }
}
fileprivate func select_from_perf(session: Session) {
    //print("select_from_perf")
    var futures = Array<Future>()
    for _ in 0 ..< NUM_CONCURRENT_REQUESTS {
        let statement = SimpleStatement(select_query).setIsIdempotent(true)
        futures.append(session.execute(statement))
    }
    for i in 0 ..< NUM_CONCURRENT_REQUESTS {
        let future = futures[i]
        if !future.wait().check(checker: checker) {
            //print("*** \(future.errorMessage)")
        }
    }
}
fileprivate func select_from_perf_prepared(session: Session) {
    //print("select_from_perf_prepared")
    let prepare = session.prepare(select_query).wait()
    prepare.check()
    let prepared = prepare.prepared
    var futures = Array<Future>()
    for _ in 0 ..< NUM_CONCURRENT_REQUESTS {
        let statement = prepared.statement.bind().setIsIdempotent(true)
        futures.append(session.execute(statement))
    }
    for i in 0 ..< NUM_CONCURRENT_REQUESTS {
        let future = futures[i]
        if !future.wait().check(checker: checker) {
            //print("*** \(future.errorMessage)")
        }
    }
}
fileprivate
func insert_into(session: Session) {
    print("insert_into...")
    let id_str = "a98d21b2-1900-11e4-b97b-e5e358e71e0d"
    let id = UUID(uuidString: id_str)!
    let title = "La Petite Tonkinoise"
    let album = "Bye Bye Blackbird"
    let artist = "Joséphine Baker"
    let statement = SimpleStatement(insert_query, id, title, album, artist, tags)
    session.execute(statement).wait().check()
    print("...insert_into")
}

func perf() {
    print("perf...")

    var group = DispatchGroup()

    LogMessage.setLevel(.info)
    let session = getSession()
    defer {
        session.close().wait()
    }
    drop_keyspace(session: session)
    create_keyspace(session: session)
    create_table(session: session)
    insert_into(session: session)

    var run: (Session) -> ()
    switch (DO_SELECTS, USE_PREPARED) {
    case (true, true):
        print("prepared select")
        run = select_from_perf_prepared
    case (true, false):
        print("select")
        run = select_from_perf
    case (false, true):
        print("prepared insert")
        run = insert_into_perf_prepared
    case (false, false):
        print("insert")
        run = insert_into_perf
    }
    print()
    for i in 0 ..< NUM_THREADS {
        queues.append(DispatchQueue(label: "perf_\(i)", qos: .background))
    }
    for i in 0 ..< NUM_THREADS {
        queues[i].async {
            //print("queue: \(queues[i])")
            group.enter()
            for _ in 0 ..< NUM_ITERATIONS {
                run(session)
            }
            group.leave()
        }
    }
    var metrics = session.metrics
    var timeout = DispatchTime.now() + .seconds(DELTA_SECS)
    while .timedOut == group.wait(timeout: timeout) {
        metrics = session.metrics
        print(String(format:"rate stats (requests/second): mean %f 1m %f 5m %f 15m %f",
                     metrics.requests.mean_rate,
                     metrics.requests.one_minute_rate,
                     metrics.requests.five_minute_rate,
                     metrics.requests.fifteen_minute_rate))
        timeout = timeout + .seconds(DELTA_SECS)
    }
    print()
    metrics = session.metrics
    print(String(format:"final stats (microseconds): min %llu max %llu median %llu 75th %llu 95th %llu 98th %llu 99th %llu 99.9th %llu",
                 metrics.requests.min,
                 metrics.requests.max,
                 metrics.requests.median,
                 metrics.requests.percentile_75th,
                 metrics.requests.percentile_95th,
                 metrics.requests.percentile_98th,
                 metrics.requests.percentile_99th,
                 metrics.requests.percentile_999th))
    print("...perf")
}
