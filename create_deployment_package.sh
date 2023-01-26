#!/bin/bash

# This script creates a deployment package for the Lambda function.

DIST_DIR=./dist

pip install -r requirements.txt --target $DIST_DIR
cp *.py $DIST_DIR
pushd $DIST_DIR
zip -r9 ${OLDPWD}/lambda_function.zip .
popd
