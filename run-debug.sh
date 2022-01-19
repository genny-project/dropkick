#!/bin/bash
#export DDT_URL=http://internmatch.genny.life:8280
export GENNY_SHOW_VALUES=TRUE
export GENNY_SERVICE_USERNAME=service
export GENNY_KEYCLOAK_URL=https://keycloak.gada.io
export GENNY_API_URL=http://internmatch.genny.life:8280

export INFINISPAN_HOST='0.0.0.0'
export INFINISPAN_USERNAME=genny
export INFINISPAN_PASSWORD=password

./mvnw  quarkus:dev -Ddebug=5557

