import pyspark
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("ClintoxAnalysis").getOrCreate()
clintox_data = spark.read.parquet('brick/clintox.parquet')

total_count = clintox_data.count()

fda_clinical_counts = clintox_data.groupBy('FDA_APPROVED', 'CT_TOX').count() \
    .withColumn("Proportion", F.col("count") / total_count)
fda_clinical_counts.show()

fda_approved_clinically_toxic = clintox_data.filter((F.col('FDA_APPROVED') == 1) & (F.col('CT_TOX') == 1)).count()
notfda_nontoxic = clintox_data.filter((F.col('FDA_APPROVED') == 0) & (F.col('CT_TOX') == 0)).count()

print(f"FDA Approved but Clinically Toxic: {fda_approved_clinically_toxic}")
print(f"Not FDA Approved and Not Clinically Toxic: {notfda_nontoxic}")
