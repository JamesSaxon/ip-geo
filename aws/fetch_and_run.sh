#!/bin/bash 

echo "Running" $SCRIPT $ARGS

aws s3 cp $SCRIPT script

if [ ! -z "$EXTRA_FILES" ]; then
  for f in $EXTRA_FILES; do 
    aws s3 cp $f .
  done
fi

chmod +x script

echo "Beginning execution..."
./script $ARGS

