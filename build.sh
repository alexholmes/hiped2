#!/bin/bash

mvn package
cd target
rm -rf hip-2.0.0
tar -xzf hip-2.0.0-package.tar.gz
