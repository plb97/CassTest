//
//  udt.swift
//  CassTest
//
//  Created by Philippe on 17/01/2018.
//  Copyright © 2018 PLHB. All rights reserved.
//


import Cass

fileprivate
func getSession() -> Session {
    let session = Session()
    session.connect(Cluster().setContactPoints("127.0.0.1").setCredentials())
        .wait()
        .check()
    return session
}

fileprivate
func create_keyspace(session: Session) {
    print("create_keyspace...")
    let query = """
    CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {
                           'class': 'SimpleStrategy', 'replication_factor': '3' };
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_keyspace")
    future.check()
}
fileprivate
func create_types(session: Session) {
    print("create_types...")
    session.execute(SimpleStatement("CREATE TYPE IF NOT EXISTS examples.phone_numbers (phone1 int, phone2 int);")).wait().check()
    session.execute(SimpleStatement("CREATE TYPE IF NOT EXISTS examples.address (street text, city text, zip int, phone set<frozen<phone_numbers>>);")).wait().check()
    print("...create_types")
}
fileprivate
func create_table(session: Session) {
    print("create_table...")
    let query = """
    CREATE TABLE IF NOT EXISTS examples.udt (id timeuuid, address frozen<address>, PRIMARY KEY(id));
    """
    let future = session.execute(SimpleStatement(query)).wait()
    print("...create_table")
    future.check()
}
fileprivate
func insert_into(session: Session, schema_meta: SchemaMeta) {
    print("insert_into...")
    let query = "INSERT INTO examples.udt (id, address) VALUES (?, ?);"
    if let keyspace_meta = schema_meta.keyspaceMeta(keyspace: "examples") {
        let gen = UuidGen()
        let id = gen.time
        print("id=\(id)")
        let id_str = id.string
        print("id_str=\(id_str)")
        let id_time_and_version = id.time_and_version
        print("id_time_and_version=\(id_time_and_version)")
        let zip = Int32(truncatingIfNeeded: id_time_and_version)
        print("zip=\(zip)")
        let udt_address = keyspace_meta.userType(name: "address")
        let udt_phone_numbers = keyspace_meta.userType(name: "phone_numbers")
        var phone = Set<UserType>()
        let address = UserType(dataType: udt_address)
        address["street"] = id_str
        address["city"] = id_str
        address["zip"] = zip
        //address.setString(name: "street", id_str).setString(name: "city", id_str).setInt32(name: "zip", zip)
        for i in 0..<2 {
            let phone_numbers = UserType(dataType: udt_phone_numbers,("phone1",i + 1),("phone2",i + 2))
            print("phone_numbers[\(i)]=\(phone_numbers)")
            phone.insert(phone_numbers)
        }
        address["phone"] = phone
        //address.setCollection(name: "phone", phone)
        print("address=\(address)")
        let statement = SimpleStatement(query, id, address)
        session.execute(statement).wait().check()
    }
    print("...insert_into")
}
fileprivate
func select_from(session: Session) {
    print("select_from...")
    let query = "SELECT id, address FROM examples.udt"
    let statement = SimpleStatement(query)
    let future = session.execute(statement).wait()
    future.check()
    let rs = future.result
     print("rows")
     for row in rs.rows {
        if let id = row.any(0) as? UUID, let address = row.any(1) as? UserType {
            for name in address.names {
                if let value = address[name] {
                    if "phone" == name {
                        if let phones = value as? Set<UserType> {
                            for phone in phones {
                                for tel_nam in phone.names {
                                    if let tel_num = phone[tel_nam] {
                                        print("id \(id) \(name): \(tel_nam) \(tel_num)")
                                    }
                                }
                            }
                        }

                    } else {
                        print("id \(id) \(name): \(value)")
                    }
                }
            }
        }
     }
    print("...select_from")
}

func udt() {
    print("udt...")
    let session = getSession()
    defer {
        session.close().wait()
    }

    create_keyspace(session: session)
    create_types(session: session)
    create_table(session: session)
    let schemaMeta = session.schemaMeta
    insert_into(session: session, schema_meta: schemaMeta)
    select_from(session: session)

    print("...udt")
}
