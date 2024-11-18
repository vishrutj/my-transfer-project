# README

This is sample dbt repo for interview purpose

Please provide answers for these following questions:
* What are each of the files for?
m0 and m1 are reporting sql files. 
m0 - Payment transactions
m1 - Payment Tax Details

* How to run this project and what results we expect.
Expected Results:
- Staging: Cleaned, deduplicated source data
- Intermediate: Processed payment and service information
- Marts: Two main reports
  1. Payment Transactions (former m0_report)
  2. Payment Tax Details (former m1_report)


List some of this solution's design problems and compare to dbt projects best practices.
Original Solution Problems:
- Separate DBT projects for each model
- Shell Script Execution
- Environment Variables in Config Files
- No Clear Model Organization and separation
- Duplicate code logic
- Complex Deployment Process

Best Practices for a DBT Project:
- Modular Structure
- Environment handling

This project runs every model as a separated dbt project which is inefficient.

Provide an alternative solution to put everything in one single dbt project that can handle all the environments(targets) and all models.

Improvements Over Original:
- Single Project Structure
- Environment Handling through DBT
- Clear Model Dependency Defined : Staging -> Intermediate -> Marts
- Proper Documentation
- Simpler Deployment Process:
    1. dbt run --target prod  # Builds all prod models
    2. dbt run --target preprod   # Builds all preprod(dev) models

- Easier to Maintain
- Builds Clear Data Lineage 

Requirements:
* because preprod and prod have different models, your solution should be able to handle different models based on the environment
* we only need to run one 'dbt run' per environment to build all models for that environment
* get rid of the dbt_command_loop.sh, preprod_table_config.yml and table_config.yml and provide a much simpler and cleaner solution

Provide a new repo containing the new solution's code.  
Provide your analysis and suggestions if there are many ways to achieve the same results.