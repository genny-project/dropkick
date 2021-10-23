#!/bin/bash
jps -l | grep dropkick | cut -d" " -f1 | xargs kill -9

