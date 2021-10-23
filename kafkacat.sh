#!/bin/bash
kafkacat -b kafka:9092 -C -o beginning -q -t webcmds 


