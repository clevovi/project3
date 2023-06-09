import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="chaya-bucket",
    table_name="accelerometer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1685965188168 = glueContext.create_dynamic_frame.from_catalog(
    database="chaya-bucket",
    table_name="customer_trustdata",
    transformation_ctx="AmazonS3_node1685965188168",
)

# Script generated for node customer privacy join
customerprivacyjoin_node1685965443063 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1685965188168,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="customerprivacyjoin_node1685965443063",
)

# Script generated for node Drop Fields
DropFields_node1685965628441 = DropFields.apply(
    frame=customerprivacyjoin_node1685965443063,
    paths=[
        "timestamp",
        "customername",
        "email",
        "phone",
        "birthdate",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1685965628441",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1685965628441,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://chaya-bucket/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
