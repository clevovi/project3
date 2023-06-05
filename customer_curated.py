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

# Script generated for node CustomerTrusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="chaya-bucket",
    table_name="customer_trustdata",
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer_landing
Accelerometer_landing_node1685965188168 = glueContext.create_dynamic_frame.from_catalog(
    database="chaya-bucket",
    table_name="accelerometer_landing",
    transformation_ctx="Accelerometer_landing_node1685965188168",
)

# Script generated for node join
join_node1685965443063 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=Accelerometer_landing_node1685965188168,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="join_node1685965443063",
)

# Script generated for node Drop Fields
DropFields_node1685965628441 = DropFields.apply(
    frame=join_node1685965443063,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1685965628441",
)

# Script generated for node CustomerCurated
CustomerCurated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1685965628441,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://chaya-bucket/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerCurated_node3",
)

job.commit()
