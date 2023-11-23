#!/bin/bash
while true; do
    secs_running=$(mongosh --quiet --eval "db.currentOp().inprog.filter(op => op.desc === 'conn6').map(op => op.secs_running)[0]")
    
    if [ -z "$secs_running" ]; then
        echo "Operation conn6 is no longer running."
        break
    else
        echo "Operation conn6 running time: $secs_running seconds"
    fi

    sleep 60  # Check every minute
done
