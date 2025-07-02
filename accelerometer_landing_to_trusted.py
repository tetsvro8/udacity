import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1751438898761 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://test-250623-tetsvro/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1751438898761")

# Script generated for node Amazon S3
AmazonS3_node1750660066439 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://test-250623-tetsvro/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1750660066439")

# Script generated for node Join
Join_node1751438907284 = Join.apply(frame1=AmazonS3_node1751438898761, frame2=AmazonS3_node1750660066439, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1751438907284")

# Script generated for node Select Fields
SelectFields_node1751439447097 = SelectFields.apply(frame=Join_node1751438907284, paths=["user", "x", "y", "z", "timestamp"], transformation_ctx="SelectFields_node1751439447097")

# Script generated for node accelerometer-trusted-zone
EvaluateDataQuality().process_rows(frame=SelectFields_node1751439447097, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750655633758", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometertrustedzone_node1750660111734 = glueContext.getSink(path="s3://test-250623-tetsvro/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometertrustedzone_node1750660111734")
accelerometertrustedzone_node1750660111734.setCatalogInfo(catalogDatabase="test-250623-tetsvro",catalogTableName="accelerometer_trusted")
accelerometertrustedzone_node1750660111734.setFormat("json")
accelerometertrustedzone_node1750660111734.writeFrame(SelectFields_node1751439447097)
job.commit()