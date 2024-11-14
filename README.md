# README

This is sample dbt repo for interview purpose

Please provide answers for these following questions:
* What are each of the files for?
* How to run this project and what results we expect.

List some of this solution's design problems and compare to dbt projects best practices.

This project runs every model as a separated dbt project which is inefficient.

Provide an alternative solution to put everything in one single dbt project that can handle all the environments(targets) and all models.

Requirements:
* because preprod and prod have different models, your solution should be able to handle different models based on the environment
* we only need to run one 'dbt run' per environment to build all models for that environment
* get rid of the dbt_command_loop.sh, preprod_table_config.yml and table_config.yml and provide a much simpler and cleaner solution

Provide a new repo containing the new solution's code.  
Provide your analysis and suggestions if there are many ways to achieve the same results.