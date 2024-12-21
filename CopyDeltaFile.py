from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit
import datetime

####################################################
# Copy a delta file, primarely though the verisons, but also via delta calculation if some versions are erased.


# Initialize Spark Session with Delta Lake
spark = SparkSession.builder \
    .appName("DeltaTableSync") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

class DeltaTableSync:
    def __init__(self, source_path, target_path):
        self.source_path = source_path
        self.target_path = target_path
        self.source_table = DeltaTable.forPath(spark, self.source_path)
        self.target_table = DeltaTable.forPath(spark, self.target_path)

    def _get_missing_versions(self):
        """Get the versions that are in source but not in target."""
        source_versions = [int(v.version) for v in self.source_table.history().collect()]
        target_versions = [int(v.version) for v in self.target_table.history().collect()]
        return sorted(set(source_versions) - set(target_versions))

    def _copy_missing_versions(self):
        """Copy missing versions from source to target table."""
        missing_versions = self._get_missing_versions()
        for version in missing_versions:
            source_df = spark.read.format("delta").option("versionAsOf", version).load(self.source_path)
            source_df.write \
                .format("delta") \
                .mode("append") \
                .save(self.target_path)

    def _find_and_apply_diffs(self):
        """Identify differences between latest versions of source and target, apply updates or inserts."""
        source_latest = self.source_table.toDF()
        target_latest = self.target_table.toDF()

        # Identify records that are in source but not in target or have different values
        diff = source_latest.join(target_latest, ['id'], 'leftanti').union(
            source_latest.join(target_latest, ['id'], 'left').filter(
                (source_latest['column_name'] != target_latest['column_name']) |
                (source_latest['another_column'] != target_latest['another_column'])
            ).select(source_latest['*'])
        )

        # Use MERGE to update or insert the differences
        self.target_table.alias("tgt").merge(
            diff.alias("src"),
            "tgt.id = src.id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    def sync_tables(self):
        """Public method to sync the target table to match the source table as closely as possible."""
        self._copy_missing_versions()
        self._find_and_apply_diffs()

# Usage
if __name__ == "__main__":
    source_path = "/path/to/source/delta/table"
    target_path = "/path/to/target/delta/table"
    
    sync_manager = DeltaTableSync(source_path, target_path)
    sync_manager.sync_tables()

    spark.stop()
