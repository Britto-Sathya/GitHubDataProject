import sys
from pyspark.sql import *
from pyspark.sql.functions import explode,col,round,avg,sum,unix_timestamp,current_timestamp

spark = SparkSession\
         .builder\
         .appName("Program Read Git Files")\
         .master("local[*]")\
         .getOrCreate()

print(spark.sparkContext.appName)

## Read the PULL REQUEST data file and ISSUES data fu=ile in to two data frames ##

df_pr = spark.read.option("multiline","true").json("/Users/britto_sathya/Downloads/data-main/prepared_pull_requests.json")
df_issue = spark.read.option("multiline","true").json("/Users/britto_sathya/Downloads/data-main/prepared_issues.json")

## Fact table that contains Pull Request realted facts and issues related facts  ##
## with Pull Request Id as key is loaded into the Data frame df_fact_pr          ##

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

## Remove any duplicate record on the column pr_id as this is the unique key in the fact table and also the dist key ##
df_fact_pr = df_fact_pr.dropDuplicates(['pr_id'])

## Dimension table that contains Issues realted metadata                      ##
## can be joined with other fact and dimension table using Issues Id          ##

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


## Remove any duplicate record on the column issue_id as this is the unique key in the Issues dimension table and also the dist key ##
df_dim_issues = df_dim_issues.dropDuplicates(['issue_id'])


## Dimension table that contains all Lables of pull request Id                ##
## can be joined with other fact and dimension tables using Pull Request Id          ##

df_dim_label = df_pr.select(df_pr['id'].alias('pr_id'),df_pr['number'].alias('issue_no'),explode("labels").alias("label_exp"))\
    .withColumn("label_id",col("label_exp.id"))\
    .withColumn("label_desc",col("label_exp.description"))\
    .withColumn("label_name",col("label_exp.name"))\
    .withColumn("label_color",col("label_exp.color"))\
    .withColumn("label_node_id",col("label_exp.node_id"))\
    .withColumn("label_url",col("label_exp.url"))\
    .withColumn("label_default",col("label_exp.default"))\
    .drop("assignee","assignees","auto_merge","base","head","requested_reviewers","requested_teams","user","label_exp")

df_dim_label = df_dim_label.dropDuplicates()

###  Question - Which labels are most commonly assigned to PRs, and what is their relative distribution? ###
###  Ans: The top three common lables in PRS are no-changelog, no-backport, area/frontend and they combinely contirbute ~40 % of the over all lables ##
###  We can automate this further to pick the top contributers based in the distribution % ###


df_label_count = df_dim_label.groupBy("label_id","label_name")\
                             .count()\
                             .withColumn("percentage",round((col("count") / df_dim_label.count()) * 100,2))\
                             .orderBy(col("count").desc())\
                             .limit(3)

print("Question 1")
print("The top three common lables in PRS are")
df_label_count.show()

### What is the average and median of the total number of lines changed per milestone? ###

df_pr_milestone_lines = df_fact_pr.withColumn("line_changes",col("additions") + col("deletions"))\
                                  .groupBy("milestone_id")\
                                  .sum("line_changes").alias("line_changed")


df_pr_milestone_lines_avg = df_pr_milestone_lines.agg(avg("sum(line_changes)"))
milestone_lines_median  =   df_pr_milestone_lines.approxQuantile("sum(line_changes)",[0.5],0.1)
approx_median = milestone_lines_median[0]
line_changes = df_pr_milestone_lines_avg.collect()[0][0]

print("Question 2")
print("Approximated Median of the total number of lines changed per milestone:", approx_median)
print("Average Lines Changed per Milestone:",line_changes)

### How long do pull requests (PRs) stay open on average? ###
### Considered only the PRs that are in Open state        ###
### Enhancement: Identify the stage in which the PR is taking longer and intimate the user/reviewrs accodringly ###

df_with_unix_time = df_fact_pr.withColumn("current_timestamp", current_timestamp())\
                        .withColumn("unix_created_at", unix_timestamp(col("created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
                        .withColumn("unix_closed_at", unix_timestamp(col("closed_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))\
                        .withColumn("unix_current_timestamp",unix_timestamp("current_timestamp"))\

df_with_difference = df_with_unix_time.withColumn("open_time", (col("unix_current_timestamp") - col("unix_created_at"))/86400).filter(col("state") == 'open')

df_pr_open_days_avg = df_with_difference.agg(avg("open_time"))
open_time = df_pr_open_days_avg.collect()[0][0]

print("Question 3")
print("Average Days the PRs stay open is",open_time)

### What is the average time interval between an issue being created and the corresponding PR being merged? ##

df_fact_pr = df_fact_pr.filter((col("merged_at") != "NULL") & (col("state") == 'closed'))
df_join_pr_issues = df_fact_pr.join(df_dim_issues, "issue_no", "inner").select(df_fact_pr["pr_id"], df_fact_pr["issue_no"],
                                                                                df_fact_pr["created_at"].alias("pr_created_at"),
                                                                                df_fact_pr["merged_at"].alias("pr_merged_at"),
                                                                                df_fact_pr["state"].alias("pr_state"),
                                                                                df_dim_issues["created_at"].alias("issue_created_at"))


df_issue_resolution_time = df_join_pr_issues.withColumn("unix_pr_merged_at", unix_timestamp(col("pr_merged_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
                        .withColumn("unix_issue_created_at", unix_timestamp(col("issue_created_at"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))\
                        .withColumn("issue_pr_merge_time", (col("unix_pr_merged_at") - col("unix_issue_created_at"))/3600)

df_issue_resolution_time_avg = df_issue_resolution_time.agg(avg("issue_pr_merge_time"))
issue_resolution_time = df_issue_resolution_time_avg.collect()[0][0]

print("Question 4")
print("Average time interval between an issue being created and the corresponding PR being merged in hours:",issue_resolution_time)


# REDSHIFT configured in VPC and roles defined, security groups configured for inbound traffic

# Remove Duplicates
# dropna() - Drop the rows and columsn with na values