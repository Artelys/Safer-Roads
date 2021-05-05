#!/bin/bash
cd /home/kibana/kibana/plugins/risk_prediction
yarn build
cd /home/kibana/kibana/plugins/data_management
yarn build

cp /home/kibana/kibana/plugins/data_management/build/data_management-0.0.0.zip /home/kibana/build/
cp /home/kibana/kibana/plugins/risk_prediction/build/risk_prediction-0.0.0.zip /home/kibana/build/