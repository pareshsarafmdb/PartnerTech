gcloud dataproc jobs submit spark \
    --cluster democluster --region us-central1 \
    --class dataproc.demo.etl.ETLDemo \
    --jars gs://paresh_bucket/GSIDemo-fat.jar \
    -- 1000
