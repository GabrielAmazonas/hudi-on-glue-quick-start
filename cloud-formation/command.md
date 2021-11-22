aws cloudformation deploy --profile dev --template-file GlueJobPySparkHudiResource.yaml --stack-name glue-job-hudi-resource --capabilities CAPABILITY_NAMED_IAM --parameter-overrides S3BucketName=glue-hudi-bucket HudiSparkJarName=hudi-spark3-bundle_2.12-0.9.0.jar SparkAvroJarName=spark-avro_2.12-3.0.1.jar GlueJobName=glue-job-hudi-resource HudiJobName=job.py

