#  CassTest

Première tentative **expérimentale** pour utiliser avec le langage **Swift** (version 4) d'*Apple* la base NoSQL **Cassandra** (version 9) via le pilote **cpp-driver** de *Datastax*

Tests reprenant les exemples fournis avec le pilote **cpp-driver**.

## Utilisation d'une base NoSQL Cassandra 3

### Création du conteneur Docker *cassandra_test*

docker run --name cassandra_test -p 7000:7000 -p7001:7001 -p7199:7199 -p9042:9042 -p9160:9160 -d cassandra:latest
docker logs cassandra_test

docker exec -t -i cassandra_test /bin/bash

// sauvegarder la configuration
$ cp -v /etc/cassandra/cassandra.yaml /etc/cassandra/cassandra.yaml.origin
$ cp -v /etc/init.d/cassandra /etc/init.d/cassandra.origin

// autoriser les procedures stockées utilisateur (facultatif)
$ cat /etc/cassandra/cassandra.yaml|grep "^enable_user_defined_functions:"
$ sed -i -e "s/^enable_user_defined_functions: false/enable_user_defined_functions: true #false/g" /etc/cassandra/cassandra.yaml
$ cat /etc/cassandra/cassandra.yaml|grep "^enable_user_defined_functions:"

// autoriser l'autentification par 'user/password' (facultatif)
$ cat /etc/cassandra/cassandra.yaml|grep "^authenticator:"
$ sed -i -e "s/^authenticator: AllowAllAuthenticator/authenticator: PasswordAuthenticator #AllowAllAuthenticator/g" /etc/cassandra/cassandra.yaml
$ cat /etc/cassandra/cassandra.yaml|grep "^authenticator:"

// regler le probleme de 'ulimit' (facultatif)
$ cat /etc/init.d/cassandra|grep "ulimit"
$ sed -i -e "s/ulimit/#ulimit/g" /etc/init.d/cassandra
$ cat /etc/init.d/cassandra|grep ulimit
// sortir du conteneur
$ exit

// pour rétablir l'absence d'autentification (facultatif)
docker restart cassandra_test
$ cat /etc/cassandra/cassandra.yaml|grep "^authenticator:"
$ sed -i -e "s/^authenticator: PasswordAuthenticator #AllowAllAuthenticator/authenticator: AllowAllAuthenticator/g" /etc/cassandra/cassandra.yaml
$ cat /etc/cassandra/cassandra.yaml|grep "^authenticator:"

