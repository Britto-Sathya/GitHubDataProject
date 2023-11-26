from SparkSessionManager import SparkSessionManager
from DataFrameManager import DataFrameManager
from DataAnalysisManager import DataAnalysisManager
class Driver:
    def __init__(self):
        self.session_manager = SparkSessionManager("GitHub Data Analysis Projet", "local[*]")
        self.spark = self.session_manager.get_spark_session()
        self.df_manager = DataFrameManager(self.spark)
        self.analysis_manager = DataAnalysisManager(self.df_manager)

    def main(self):
        # Read the PULL REQUEST data file and ISSUES data file in to two data frames #
        print("Reading JSON")
        df_pr = self.df_manager.read_json("/Users/britto_sathya/Downloads/data-main/prepared_pull_requests.json")
        df_issue = self.df_manager.read_json("/Users/britto_sathya/Downloads/data-main/prepared_issues.json")

        """ 
        Fact table that contains Pull Request realted facts and issues related facts
        with Pull Request Id as key is loaded into the Data frame df_fact_pr
        """
        df_fact_pr = self.df_manager.create_pull_request_fact_table(df_pr)

        # Remove any duplicate record on the column pr_id as this is the unique key in the fact table and also the dist key #
        df_fact_pr = self.df_manager.drop_duplicates(df_fact_pr, 'pr_id')

        """ 
        Dimension table that contains Issues realted metadata                      
        can be joined with other fact and dimension table using Issues Id
        """
        df_dim_issues = self.df_manager.create_issues_dimension_table(df_issue)

        # Remove any duplicate record on the column issue_id as this is the unique key in the Issues dimension table and also the dist key #
        df_dim_issues = self.df_manager.drop_duplicates(df_dim_issues, 'issue_id')

        """
        Dimension table that contains all Lables of pull request Id                
        can be joined with other fact and dimension tables using Pull Request Id  
        """
        df_dim_label = self.df_manager.create_label_distribution_dimension_table(df_pr)

        ## Remove any duplicate records from the dataframe ##
        df_dim_label = self.df_manager.drop_duplicates(df_dim_label)

        # Further processing and analysis
        print("Question 1: How long do pull requests (PRs) stay open on average?\n")
        open_time = self.analysis_manager.calculate_average_open_time(df_fact_pr)
        print("Average days the PRs stay open: ", format(open_time, '.2f'))

        print("\nQuestion 2: What is the average time interval between an issue being created and the corresponding PR being merged?\n")
        issue_resolution_time = self.analysis_manager.issue_to_pr_resolution_time(df_fact_pr, df_dim_issues)
        print("Average time interval between an issue being created and the corresponding PR being merged in hours: ", format(issue_resolution_time, '.2f'))

        print("\nQuestion 3: Which labels are most commonly assigned to PRs, and what is their relative distribution?\n")
        print("The top three common lables in PRS and their distributions in percentage are: ")
        top_labels = self.analysis_manager.get_top_labels(df_dim_label, 3)

        print("\nQuestion 4: What is the average and median of the total number of lines changed per milestone?\n")
        avg_line_changes, median_line_changes = self.analysis_manager.analyze_milestone_line_changes(df_fact_pr)
        print("Approximated Median of the total number of lines changed per milestone: ", format(avg_line_changes, '.2f'))
        print("\nAverage Lines Changed per Milestone: ", format(median_line_changes, '.2f'))

if __name__ == "__main__":
    driver = Driver()
    driver.main()
