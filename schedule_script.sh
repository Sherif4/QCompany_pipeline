#!/bin/bash

export LC_ALL=C.UTF-8
export LANG=C.UTF-8


/home/itversity/.local/bin/papermill /home/itversity/itversity-material/Retail_pipeline_project/ingestion_script.ipynb /home/itversity/itversity-material/retail_pipeline_project/papermill_logs/ingestion_output.json \
	&& /home/itversity/.local/bin/papermill  /home/itversity/itversity-material/Retail_pipeline_project/Silver_layer_transformation.ipynb /home/itversity/itversity-material/Retail_pipeline_project/papermill_logs/Silver_output.json \
	&& /home/itversity/.local/bin/papermill  /home/itversity/itversity-material/Retail_pipeline_project/Gold_layer_transformation.ipynb /home/itversity/itversity-material/Retail_pipeline_project/papermill_logs/Gold_output.json

