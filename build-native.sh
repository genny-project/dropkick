#!/bin/bash
./mvnw package -Pnative -DskipTests=true
#./mvnw package -Pnative -Dquarkus.native.container-build=true -DskipTests=true
