# Master Package Example Project

## 1. Project details

This project is based on the [Pyfony framework](https://github.com/DataSentics/pyfony) extended with the following Pyfony bundles:

* [console-bundle](https://github.com/DataSentics/console-bundle) - commands for command line & Databricks automation
* [logger-bundle](https://github.com/DataSentics/logger-bundle) - logging to standard output & Azure Application Insights
* [databricks-bundle](https://github.com/DataSentics/databricks-bundle) - SparkSession & DBUtils initialization for both the **local** (DBX Connect) and **online** (Databricks UI) environments
* [dbx-deploy](https://github.com/DataSentics/dbx-deploy) - Spark-based applications deployment automation for Databricks


## 2. Local environment setup

The following software needs to be installed first:
  * [Java 8](https://www.java.com/en/download/) (Java 11 **is not** supported)
  * [PyCharm Community or Pro](https://www.jetbrains.com/pycharm/download/)
  * [Miniconda package manager](https://docs.conda.io/en/latest/miniconda.html)
  * [Git for Windows](https://git-scm.com/download/win) or standard Git in Linux (_apt-get install git_)

Clone the repo now and prepare the package environment:

* On **Windows**, use [Git Bash](docs/git-bash.png).
* On **Linux/Mac**, the use standard console 

```bash
$ git clone https://github.com/DataSentics/master-package-example.git
$ cd master-package-example
$ ./env-init.sh
```

After the environment setup is complete, activate the Conda environment:

```bash
$ conda activate $PWD/.venv
```

or use a shortcut

```bash
$ ca
```

## 3. Important scripts

1. ```./pylint.sh``` - checks coding standards
