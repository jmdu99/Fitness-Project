#!/bin/bash
set -e

echo "Starting Prefect Orion..."
prefect server start --host 0.0.0.0 &

sleep 3
echo "Applying the ETL deployment..."
python deployment.py

sleep 2
echo "Starting Agent on 'default' queue..."
prefect agent start -q default
