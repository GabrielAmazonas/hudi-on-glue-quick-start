### AWS Cloud Formation Template for AWS Glue PySpark Apache Hudi Quick Start


#### Resources included:

1. AWS::IAM::Role
    - Required for Glue to interact with S3 service.
2. AWS::S3::Bucket
    - Required to store artifacts referenced by the Glue Job. The Job Script itself and the Jars used by the Glue Job.
3. AWS::Glue::Job
    - The Glue job that runs the PySpark code and it's configuration.

