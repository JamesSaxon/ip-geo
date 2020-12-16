#!/bin/bash 

echo "Running" $SCRIPT $ARGS

aws s3 cp $SCRIPT script

chmod +x script

echo "Beginning execution..."
./script $ARGS

