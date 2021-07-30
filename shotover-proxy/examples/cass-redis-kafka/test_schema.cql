CREATE KEYSPACE "test" WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE test.simple (
    pk text,
    value text,
    PRIMARY KEY (pk)
);

CREATE TABLE test.clustering (
    pk text,
    clustering int,
    value text,
    PRIMARY KEY ( pk, clustering )
);

CREATE TABLE test.clustering2 (
    pk text,
    clustering int,
    clustering2 int,
    value text,
    PRIMARY KEY ( pk, clustering, clustering2 )
);


CREATE TABLE test.encryption (
    pk text,
    clustering int,
    value blob,
    PRIMARY KEY ( pk, clustering)
);

-- INSERT INTO test.encryption (pk, clustering, value) VALUES ('pk1', 1, 'TOPSECRET');


INSERT INTO test.simple (pk, value) VALUES ('pk1', 'value1');
INSERT INTO test.simple (pk, value) VALUES ('pk2', 'value2');
INSERT INTO test.simple (pk, value) VALUES ('pk3', 'value3');
INSERT INTO test.simple (pk, value) VALUES ('pk4', 'value3');


INSERT INTO test.clustering (pk, clustering, value) VALUES ('pk1', 1, 'value1');
INSERT INTO test.clustering (pk, clustering, value) VALUES ('pk2', 1, 'value2');
INSERT INTO test.clustering (pk, clustering, value) VALUES ('pk3', 1, 'value3');
INSERT INTO test.clustering (pk, clustering, value) VALUES ('pk3', 2, 'value3.2');
INSERT INTO test.clustering (pk, clustering, value) VALUES ('pk3', 3, 'value3.3');

INSERT INTO test.clustering2 (pk, clustering, clustering2, value) VALUES ('pk1', 1,1, 'value1');
INSERT INTO test.clustering2 (pk, clustering, clustering2, value) VALUES ('pk2', 1,1, 'value2');
INSERT INTO test.clustering2 (pk, clustering, clustering2, value) VALUES ('pk3', 1,1, 'value3');
INSERT INTO test.clustering2 (pk, clustering, clustering2, value) VALUES ('pk3', 2,2, 'value3.2');
INSERT INTO test.clustering2 (pk, clustering, clustering2, value) VALUES ('pk3', 3,3, 'value3.3');