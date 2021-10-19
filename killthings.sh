#!/bin/bash
jps -l | grep lauchy | cut -d" " -f1 | xargs kill -9

