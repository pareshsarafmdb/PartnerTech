mvn compile exec:java \
    -Dexec.mainClass=dataflowdemonew.DataflowDemoMongo \
    -Dexec.args="--project=erudite-scholar-262008 \
    --gcpTempLocation=gs://erudite-scholar-262008/tmp/ \
    --output=gs://erudite-scholar-262008/output \
    --runner=DataflowRunner \
    --jobName=dataflow-intro" \
    -Pdataflow-runner
