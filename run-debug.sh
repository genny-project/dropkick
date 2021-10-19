#!/bin/bash
#export DDT_URL=http://internmatch.genny.life:8280
export GENNY_SHOW_VALUES=TRUE
export GENNY_SERVICE_USERNAME=service
export GENNY_KEYCLOAK_URL=https://keycloak-office.gada.io
export GENNY_API_URL=http://internmatch.genny.life:8280
./mvnw clean quarkus:dev -Ddebug=5556

