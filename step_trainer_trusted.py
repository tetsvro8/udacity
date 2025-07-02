import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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
AmazonS3_node1751438898761 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://test-250623-tetsvro/step_trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1751438898761")

# Script generated for node Amazon S3
AmazonS3_node1750660066439 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://test-250623-tetsvro/customer/curated/"], "recurse": True}, transformation_ctx="AmazonS3_node1750660066439")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT 
st.sensorreadingtime, 
st.serialnumber, 
st.distancefromobject
FROM st
inner join cc 
on st.serialnumber = cc.serialnumber
'''
SQLQuery_node1751441363220 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"st":AmazonS3_node1751438898761, "cc":AmazonS3_node1750660066439}, transformation_ctx = "SQLQuery_node1751441363220")

# Script generated for node step-trainer-trusted-zone
EvaluateDataQuality().process_rows(frame=SQLQuery_node1751441363220, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1750655633758", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
steptrainertrustedzone_node1750660111734 = glueContext.getSink(path="s3://test-250623-tetsvro/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="steptrainertrustedzone_node1750660111734")
steptrainertrustedzone_node1750660111734.setCatalogInfo(catalogDatabase="test-250623-tetsvro",catalogTableName="step_trainer_trusted")
steptrainertrustedzone_node1750660111734.setFormat("json")
steptrainertrustedzone_node1750660111734.writeFrame(SQLQuery_node1751441363220)
job.commit()