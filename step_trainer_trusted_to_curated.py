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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://chaya-bucket/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1685965188168 = glueContext.create_dynamic_frame.from_catalog(
    database="chaya-bucket",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1685965188168",
)

# Script generated for node join
join_node1685965443063 = Join.apply(
    frame1=step_trainer_trusted_node1,
    frame2=accelerometer_trusted_node1685965188168,
    keys1=["timeStamp"],
    keys2=["timestamp"],
    transformation_ctx="join_node1685965443063",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=join_node1685965443063,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://chaya-bucket/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machine_learning_curated_node3",
)

job.commit()
