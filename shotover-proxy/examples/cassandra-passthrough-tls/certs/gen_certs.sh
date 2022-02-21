#!/bin/bash

# Generate localhost_CA and localhost certs/keys
openssl genrsa -out localhost_CA.key 4096
openssl req -x509 -new -config localhost_CA.cfg -key localhost_CA.key -days 9999 -out localhost_CA.crt
openssl genrsa -out localhost.key 4096
openssl req -new -config localhost.cfg -key localhost.key -days 9999 -out localhost.csr
openssl x509 -req -in localhost.csr -CA localhost_CA.crt -CAkey localhost_CA.key -CAcreateserial -days 9999 -out localhost.crt


# generate keystore
openssl pkcs12 -export -out keystore.p12 -inkey localhost.key -in localhost.crt -passout pass:password
keytool -importkeystore -destkeystore keystore.jks -srcstoretype PKCS12 -srckeystore keystore.p12 -deststorepass "password" -srcstorepass "password"

# generate truststore
openssl pkcs12 -export -out truststore.p12 -inkey localhost.key -in localhost.crt -passout pass:password
keytool -importkeystore -destkeystore truststore.jks -srcstoretype PKCS12 -srckeystore truststore.p12 -deststorepass "password" -srcstorepass "password"
