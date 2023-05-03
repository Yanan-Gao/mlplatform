# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Function to mount an S3 directory with the correct AWS role settings
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def mount(s3_path, mnt_path):
  try:
    dbutils.fs.unmount(mnt_path)
  except:
    print("mnt does not exist")

  dbutils.fs.mount(s3_path, mnt_path, extra_configs = {
    "fs.s3a.canned.acl":"BucketOwnerFullControl",
    "fs.s3a.acl.default":"BucketOwnerFullControl",
    "fs.s3a.credentialsType":"AssumeRole",
    "fs.s3a.stsAssumeRole.arn":"arn:aws:iam::003576902480:role/ttd_cluster_compute_adhoc",
  })