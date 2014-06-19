#!/bin/bash

mvn package  -DskipTests -Dmaven.javadoc.skip=true
cd target
rm -rf hip-2.0.0
tar -xzf hip-2.0.0-package.tar.gz
