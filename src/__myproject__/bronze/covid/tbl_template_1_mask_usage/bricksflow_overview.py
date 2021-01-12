# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/databricks_icon.png?raw=true" width=100/> 
# MAGIC # Databricks
# MAGIC 
# MAGIC 
# MAGIC - Easy to use environment to run a code in the cloud
# MAGIC - Interactive notebooks
# MAGIC - Built-in ML features
# MAGIC - POC in few hours/days
# MAGIC - Scalability
# MAGIC 
# MAGIC ### Challenges
# MAGIC - Sharing code across notebooks is hard
# MAGIC - Missing proper GIT support
# MAGIC - Third-party package management is hard
# MAGIC - No proper search (and replace)
# MAGIC - No code checking/auto-completion
# MAGIC - No debugger
# MAGIC - Code testing is almost impossible
# MAGIC - No environments
# MAGIC 
# MAGIC 

# COMMAND ----------

# MAGIC %md # Bricksflow
# MAGIC For an overview check Jiri's article (Bricksflow founder) - [link](https://medium.com/datasentics/bricksflow-databricks-development-made-convenient-3b0cc486c856 )
# MAGIC ## Vision & focus
# MAGIC Bricksflow is a set of best practices for Databricks projects development automated in Python and focused on the following paradigms:
# MAGIC - One code for all environments (your favorite IDE + Databricks UI)
# MAGIC - Anyone with basic python skills can create pipelines and improve the business logic
# MAGIC - Developing a standard DataLake project requires almost no engineers
# MAGIC - Pursue consistency as the project grows
# MAGIC 
# MAGIC ### Base components to be used by everyone
# MAGIC - Configuration in YAML
# MAGIC - Tables & schema management
# MAGIC - Automated deployment to Databricks
# MAGIC - Documentation automation
# MAGIC 
# MAGIC ### Advanced components to be mostly used by engineers
# MAGIC - Production releases workflow
# MAGIC - Unit & pipeline testing
# MAGIC - Extensions API

# COMMAND ----------

# MAGIC %md
# MAGIC ## What does it mean for me?
# MAGIC You will follow a standard way of creating notebooks that includes:
# MAGIC - wrap transformations into functions
# MAGIC - use @decorators
# MAGIC 
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/dbx_vs_bricksflow.PNG?raw=true" width=1200/> 

# COMMAND ----------

# MAGIC %md
# MAGIC - define parameters & tables in single place
# MAGIC - define schemas & create tables in advance
# MAGIC 
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/datalake_config.png?raw=true" width=1200/> 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/table_schema.png?raw=true" width=800/> 

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to start using it?
# MAGIC 
# MAGIC There are number of template notebooks within the Databricks Workspace on the left - see templates in folders Bronze, Silver, Gold.  Each template focus on different functionality and explains its usage in detail. You can clone template notebook and start to create your transformation in Bricksflow.
# MAGIC 
# MAGIC After your engineer set-it up you just run command `%run ../app/install_master_package` , The Master Package’s wheel gets installed to the notebook’s scope and you can start using Bricksflow functionality right away.
# MAGIC 
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/bricksflow_base.png?raw=true" width=600/> 