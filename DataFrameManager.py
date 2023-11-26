from pyspark.sql.functions import explode,col,round,avg,sum,unix_timestamp,current_timestamp
class DataFrameManager:
    def __init__(self, spark_session):
        self.spark = spark_session

    def read_json(self, path):
        return self.spark.read.option("multiline", "true").json(path)

    def create_pull_request_fact_table(self, df_pr):
        df_fact_pr = df_pr.select(
            df_pr['id'].alias('pr_id'),
            df_pr['number'].alias('issue_no'),
            df_pr['created_at'].alias('created_at'),
            df_pr['closed_at'].alias('closed_at'),
            df_pr['merged_at'].alias('merged_at'),
            df_pr['state'].alias('state'),
            df_pr['comments'].alias('comments'),
            df_pr['additions'].alias('additions'),
            df_pr['commits'].alias('commits'),
            df_pr['deletions'].alias('deletions'),
            df_pr['changed_files'].alias('changed_files'),
            col("milestone.id").alias('milestone_id'),
            col("user.id").alias('user_id'))
        return df_fact_pr

    def create_issues_dimension_table(self, df_issue):
        df_dim_issues = df_issue.select(
            df_issue['id'].alias('issue_id'),
            df_issue['number'].alias('issue_no'),
            df_issue['title'].alias('title'),
            df_issue['created_at'].alias('created_at'),
            df_issue['closed_at'].alias('closed_at'),
            df_issue['updated_at'].alias('updated_at'),
            df_issue['state'].alias('state'),
            df_issue['comments'].alias('comments'),
            df_issue['closed_by'].alias('closed_by'),
            df_issue['url'].alias('url'),
            df_issue['locked'].alias('locked'),
            df_issue['state_reason'].alias('state_reason'),
            col("milestone.id").alias('milestone_id'),
            col("user.id").alias('user_id'))
        return df_dim_issues

    def create_label_distribution_dimension_table(self, df_pr):
        df_dim_label = df_pr.select(df_pr['id'].alias('pr_id'), df_pr['number'].alias('issue_no'),
                                    explode("labels").alias("label_exp")) \
            .withColumn("label_id", col("label_exp.id")) \
            .withColumn("label_desc", col("label_exp.description")) \
            .withColumn("label_name", col("label_exp.name")) \
            .withColumn("label_color", col("label_exp.color")) \
            .withColumn("label_node_id", col("label_exp.node_id")) \
            .withColumn("label_url", col("label_exp.url")) \
            .withColumn("label_default", col("label_exp.default")) \
            .drop("assignee", "assignees", "auto_merge", "base", "head", "requested_reviewers", "requested_teams",
                  "user", "label_exp")
        return df_dim_label

    def drop_duplicates(self, df, col_name=None):
        if col_name:
            return df.dropDuplicates([col_name])
        else:
            return df.dropDuplicates()

    #def drop_duplicates(self, df):
        #return df.dropDuplicates()
