//
//  ssl.swift
//  CassTest
//
//  Created by Philippe on 05/02/2018.
//  Copyright © 2018 PLHB. All rights reserved.
//

import Cass

fileprivate let checker = {(_ err: Cass.Error) -> Bool in
    if .ok != err {
        print("*** CHECKER: Error=\(err)")
        return false
    }
    return true
}

let server_cert = """
Bag Attributes
friendlyName: cassandra
localKeyID: 54 69 6D 65 20 31 35 31 37 36 35 36 35 34 36 39 33 35
subject=/C=None/L=None/O=None/OU=None/CN=cassandra
issuer=/C=None/L=None/O=None/OU=None/CN=cassandra
-----BEGIN CERTIFICATE-----
MIIDQTCCAimgAwIBAgIERUMPujANBgkqhkiG9w0BAQsFADBQMQ0wCwYDVQQGEwRO
b25lMQ0wCwYDVQQHEwROb25lMQ0wCwYDVQQKEwROb25lMQ0wCwYDVQQLEwROb25l
MRIwEAYDVQQDEwljYXNzYW5kcmEwIBcNMTgwMjAzMTExNTE3WhgPMjExODAxMTAx
MTE1MTdaMFAxDTALBgNVBAYTBE5vbmUxDTALBgNVBAcTBE5vbmUxDTALBgNVBAoT
BE5vbmUxDTALBgNVBAsTBE5vbmUxEjAQBgNVBAMTCWNhc3NhbmRyYTCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAIiXF89d2+6Sf/JZJKfGmi42GwZZRh98
CySbQy6FXQDHGwpqjpDwIYDfAXcE6AUXXQ6YuamMzWPaeK1UsS2IozwcUiL3gMMp
KlYy9MnWTUNLNTET6SahxYynv2BlSCXhHO96zs+8GPMojRkjOJqrIj9ESR1OXQcZ
HoPc0dgWWuCu+APZkAJLvE847TCc1Z5e5/J5BTR/NmMCsV57HC0FenXWwgJnE7fa
50FqXkzXjjHolW39nGt6/EbWe0waXZJ/j4hZjGjCj8+XRRK/sTAx+1zhQ9kwlYQW
CQu7XKa80THOkD36oaCtv5ax9iGn0KnRnhSJ5xIFCmL11efBOkSO02UCAwEAAaMh
MB8wHQYDVR0OBBYEFLslsSghbI9amLjaQe25b+883mZjMA0GCSqGSIb3DQEBCwUA
A4IBAQBvqH0tweVAk/O2gs6f6kiw2QvLaA5QcKgZwV2u9do4k810n6+tvnS+MB2G
yGJ2ia5p4VKQQJF0yCNXBDidbj4dyO0v3jLCWC00qYSfXE/AH8ZIMAxXfmORdWgQ
FaucCx/l6rY4t38kmgPTHGnpzP4f58M5stGSWBZv2yaU8AXY6yANxg+SCPdYjEH/
gDQ+EX6gshFb5W9ly9ymbt26QN9ca2Zylb0h8/3gM+n9LuhaKx92YsrDltx1YkAk
Erk/tdxJT+SfQW8fzfRrEK4nl25SlIoswGt7RXk3pFtKJhLWwKyce2BSvmXK6sgJ
8BEWCbYfgdQGs6iw5gZG9K0gS4+t
-----END CERTIFICATE-----
"""
let client_cert = """
Bag Attributes
friendlyName: driver
localKeyID: 54 69 6D 65 20 31 35 31 37 36 35 36 36 32 30 36 36 31
subject=/C=None/L=None/O=None/OU=None/CN=driver
issuer=/C=None/L=None/O=None/OU=None/CN=driver
-----BEGIN CERTIFICATE-----
MIIDOzCCAiOgAwIBAgIESr2zMzANBgkqhkiG9w0BAQsFADBNMQ0wCwYDVQQGEwRO
b25lMQ0wCwYDVQQHEwROb25lMQ0wCwYDVQQKEwROb25lMQ0wCwYDVQQLEwROb25l
MQ8wDQYDVQQDEwZkcml2ZXIwIBcNMTgwMjAzMTExNjIwWhgPMjExODAxMTAxMTE2
MjBaME0xDTALBgNVBAYTBE5vbmUxDTALBgNVBAcTBE5vbmUxDTALBgNVBAoTBE5v
bmUxDTALBgNVBAsTBE5vbmUxDzANBgNVBAMTBmRyaXZlcjCCASIwDQYJKoZIhvcN
AQEBBQADggEPADCCAQoCggEBAJX1hK5U90xKaW9UqRDnyFs/h2PoYu/9CzZnuESk
7AM3TVxsYPnTiuoinQG6MgQwjf7P12Je3JqGI2paHDJN4uK1pvavXO9PjDar5gmU
HvLigEPiWZssa1jWUUL4/EGxrM0JTKZhTjBGzqZybA+rzpTwj8/RSY7M/xftHOkJ
Z038oUpm8t/2TVK0qdvslABYc9hRDcTj+ky+KGB2fkQC/58GIgf9h5Ph+Wh1kGIM
rxJGylgJ1U5ploC8Y6TIx2HWshSb5hRY94d5u3P/RUq37G6giVowwJGGB34ij4mU
hLhy47zAPBVVgR4HkOF8ymvWhM6vfn7JUUJ1etZ9HGwpxg0CAwEAAaMhMB8wHQYD
VR0OBBYEFCceEZYFPNqYvG+ynC33Byn2jG6CMA0GCSqGSIb3DQEBCwUAA4IBAQCH
S42BaFTlWlSK+oQYWSce3To9Tf3SoOL6vDoBnZA0PRf/oFNZsn4PUkfSHqjoDgkO
UHbe3hoC0vqAoJPanCoCkmKuC1L7qzIrEIc1P+kn1Q5D0nFV1YzRzWpfaFtc6Nua
3Cd0sVSdMBBmHWydPqoG/QIiGJoE62D9d25ZvrTkjNg9DK1bYH/cD/vAW5VBECKv
ms4VeqQjGZGweVzZUJYXBr76lxZDrOk7ZpgfkETtZqnDpHhIaYztdI3rr32DbJtg
DEJ5r0+AJsVk1Ef27VJvQZ83bEKaftM0F1tGbDv1hVCoILYt1jFDqYCgr/74mc5l
cnE3MlfhTPj/y+GIEuGP
-----END CERTIFICATE-----
"""
let client_key = """
Bag Attributes
friendlyName: driver
localKeyID: 54 69 6D 65 20 31 35 31 37 36 35 36 36 32 30 36 36 31
Key Attributes: <No Attributes>
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCV9YSuVPdMSmlv
VKkQ58hbP4dj6GLv/Qs2Z7hEpOwDN01cbGD504rqIp0BujIEMI3+z9diXtyahiNq
WhwyTeLitab2r1zvT4w2q+YJlB7y4oBD4lmbLGtY1lFC+PxBsazNCUymYU4wRs6m
cmwPq86U8I/P0UmOzP8X7RzpCWdN/KFKZvLf9k1StKnb7JQAWHPYUQ3E4/pMvihg
dn5EAv+fBiIH/YeT4flodZBiDK8SRspYCdVOaZaAvGOkyMdh1rIUm+YUWPeHebtz
/0VKt+xuoIlaMMCRhgd+Io+JlIS4cuO8wDwVVYEeB5DhfMpr1oTOr35+yVFCdXrW
fRxsKcYNAgMBAAECggEAZLFVhFHdNEcLAQyR8Z4RdOP1n2pddNQvATsoCj/LkmVB
9vt3KomXT5wqXQyPpSyNTDp3X8Be1QuZIEKdiFGwNZbQI/igqLh7aAKJFol1NpZM
lkiY7o8nDaHrMtNJXztU0uCZrWbNP7Fr9WwDj4pHzs6xYlQf1llkOMaokPoVedFO
Ov5ZvGDAp4H52hod3Pye3+dJW5UlQa1nfDbeHTz1bR+MiWvp3R++a8QIXbW1k1U6
d9r0RE/4iAlsHVdtAjSJQxV+lh/cKDpQs68KC4uMhQp1FtIDAoqr+x4GFiGLeOUY
rEgAU14v5zJN90ldE/7pMkG+KY8PziIZ6W9l3H7waQKBgQDaQG+fQ21XcSie6g4w
WntU+Mv8as8hkBn7kHQVwjGGOOyW9I3a8Of7aUwYw70CH7VwKxXvgNcQMKY7e43L
c4OMw5m0qTOE9WBCE6MDLnnDrsmTxbjwKJIyejqyrdgkdb8gSXNiXRbv/I21Wbxn
+yR3SI9ewjpZ6wrFOvja0V/NNwKBgQCv5Uc93KDORlXf/R6l+KpnyQRnMC6CEIwn
c3zT018ukzm0KhAxIJ9uAfCnZG2C5LTCDmQ7ESJWVLqmAo8f8VdBhXci16ZNEkD6
/OJxlqSdWYSMpmz7rS4/TNgXMbgP06gVrU6pdlqKiZUxR+1Q4Bw2loB1Hdpc/NE/
FjKef1CI2wKBgQDExZmdvr4DO1vDQwS95aYSayoGjwnmmbRDUe7He0kJWaM9Sonm
3pJY4ougPEHZe1srIT1qrP+5chM9x9yElMYDEIsxDegMNOqvSGMNSEax/ZFyMK7X
n8yHxBnN5NzdqmdCJdbo1UML5eihm6E5In2zKfFRHs5bKYWRYuTg7CpyqwKBgE9u
L2S9LBTaaE81A9EZYQJrIUTj2iy8Aj6ShW02x4XF0EoOsK8utzA7SgydamKwmvwC
+bo+u43Wx07OWwmmt6uL9MfUMHIfax6scimvSHkSAqxtP0vL4dWOAws+VYs4HnBj
ieE25B4pkSjm/UEHY1Y3Up0QtRpGxMtDX+p5Pcn/AoGACmSCMGKURYY8uED9zj/G
YP6WR3mqr5WsSwHOntGfHZ2f8ujdzJ0aNhV8kHAz+VERMLMsGIMrFlOV3Ebm+Kvw
4p87TdenF1JTL8CeHWvbNAhH56Jo1T5EAl9bPo2N3+l8CAjy+ydyq+i/3vuGM1Cx
Zk5AMV2ak+JSmJFHgvKHh3M=
-----END PRIVATE KEY-----
"""
fileprivate
func getSession() -> Session {
    let session = Session()
    let ssl = Ssl()
        .setVerifyFlags(.peerCert)
        .addTrustedCert(server_cert)
        .setCert(client_cert)
        .setPrivateKey(key: client_key)
        .setChecker(okPrintChecker)
    ssl.check()
    let future = session.connect(Cluster().setContactPoints(HOSTS).setSsl(ssl))
        .setChecker(okPrintChecker)
        .wait()
    if future.check() {
        return session
    } else {
        print("\(future.errorMessage)")
        fatalError(future.errorMessage)
    }
}

fileprivate
func select_from(session: Session) -> Result {
    let query = "SELECT release_version FROM system.local;"
    let future = session.execute(SimpleStatement(query))
        .setChecker(okPrintChecker)
        .wait()
    if future.check() {
        return future.result
    } else {
        print("\(future.errorMessage)")
        fatalError(future.errorMessage)
    }
}

func ssl() {
    print("ssl...")
    let session = getSession()
    defer {
        session.close().wait()
    }
    let rs = select_from(session: session)
    if let row = rs.first {
        //let release_version = row.any(0) as! String
        let release_version = row.any(name:"release_version") as! String
        print("release_version: \(release_version)")
    } else {
        fatalError("select error")
    }
    print("...ssl")
}

