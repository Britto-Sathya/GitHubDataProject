import DataFrameManager
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col, round, avg, sum, unix_timestamp, current_timestamp
from pyspark.sql.session import SparkSession

class DataAnalysisManager:
    def __init__(self, df_manager):
        self.df_manager = df_manager

    def get_top_labels(self, df_dim_label, top_n):
        """
        Method to find the top N labels assigned to PRs and their distribution.
        """
        df_label_count = df_dim_label.groupBy("label_id", "label_name") \
                                     .count() \
                                     .withColumn("percentage", round((col("count") / df_dim_label.count()) * 100, 2)) \
                                     .orderBy(col("count").desc()) \
                                     .limit(top_n)
        df_label_count = df_label_count.select(df_label_count['label_name'],df_label_count['percentage'])
        return df_label_count.show()

    def analyze_milestone_line_changes(self, df_fact_pr):
        """
        Analyze average and median of the total number of lines changed per milestone.
        """
        df_pr_milestone_lines = df_fact_pr.withColumn("line_changes", col("additions") + col("deletions")) \
                                          .groupBy("milestone_id") \
                                          .sum("line_changes").alias("line_changed")

        avg_line_changes = df_pr_milestone_lines.agg(avg("sum(line_changes)")).collect()[0][0]
        milestone_lines_median = df_pr_milestone_lines.approxQuantile("sum(line_changes)", [0.5], 0.1)
        median_line_changes = milestone_lines_median[0]

        return avg_line_changes, median_line_changes

    def calculate_average_open_time(self, df_fact_pr):
        """
        Calculate the average time pull requests stay open.
        """
        df_with_unix_time = df_fact_pr.withColumn("current_timestamp", current_timestamp()) \
                                      .withColumn("unix_created_at", unix_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
                                      .withColumn("unix_closed_at", unix_timestamp(col("closed_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
                                      .withColumn("unix_current_timestamp", unix_timestamp("current_timestamp"))

        df_with_difference = df_with_unix_time.withColumn("open_time", (col("unix_current_timestamp") - col("unix_created_at")) / 86400).filter(col("state") == 'open')
        avg_open_time = df_with_difference.agg(avg("open_time")).collect()[0][0]

        return avg_open_time

    def issue_to_pr_resolution_time(self, df_fact_pr, df_dim_issues):
        """
        Calculate average time interval between an issue being created and the corresponding PR being merged.
        """
        df_fact_pr = df_fact_pr.filter((col("merged_at") != "NULL") & (col("state") == 'closed'))
        df_join_pr_issues = df_fact_pr.join(df_dim_issues, "issue_no", "inner").select(
            df_fact_pr["pr_id"], df_fact_pr["issue_no"],
            df_fact_pr["created_at"].alias("pr_created_at"),
            df_fact_pr["merged_at"].alias("pr_merged_at"),
            df_fact_pr["state"].alias("pr_state"),
            df_dim_issues["created_at"].alias("issue_created_at"))

        df_issue_resolution_time = df_join_pr_issues.withColumn("unix_pr_merged_at", unix_timestamp(col("pr_merged_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
                                                    .withColumn("unix_issue_created_at", unix_timestamp(col("issue_created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
                                                    .withColumn("issue_pr_merge_time", (col("unix_pr_merged_at") - col("unix_issue_created_at")) / 3600)

        avg_resolution_time = df_issue_resolution_time.agg(avg("issue_pr_merge_time")).collect()[0][0]

        return avg_resolution_time
