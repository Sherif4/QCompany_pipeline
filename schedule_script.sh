#!/bin/bash
# Add the path to papermill
export LC_ALL=C.UTF-8
export LANG=C.UTF-8
export PATH=$PATH:/home/itversity/.local/bin:/opt/hadoop/bin   # Include Hadoop bin directory
export CLASSPATH=$(/opt/hadoop/bin/hadoop classpath --glob)   # Set Hadoop classpath
# Run papermill with full path
/home/itversity/.local/bin/papermill /home/itversity/itversity-material/retail_pipeline/ingestion_script.ipynb /home/itversity/itversity-material/retail_pipeline/execoutput.json


echo 'Cron is working 1' >> /home/itversity/itversity-material/retail_pipeline/test_output.log
