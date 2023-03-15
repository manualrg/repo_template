# Repo template
The goal of this repository is to establish a development template by setting a clean code architecture applied to
data focused Python developments (data science and data engineering). In addition, it is aimed at being a common and 
shared way of reusing code efficiently

# Prerequisites
1. Set up a conda  environment (Ubuntu)
Create environment from file:  
```conda env create -f env_conda.yaml```  
Check environment    
```conda env list```  
Activate environment  
```conda activate <name>```  

2. Configure CI loop in source code development  

* Create a src/pyproject.toml  
```
[build-system]
requires = ["setuptools"]
build-backend = "setuptools.build_meta"
```

* Create a src/setup.cfg  
```
[metadata]
name = example
version = 0.1.0

[options]
packages = find:
```

* Install your own source code on your current conda environment as a package in editable mode,
  in the same folder as setup.cfg
```pip install -e .```
* Test that you can import src/ from other folders than the root 
  (run test/example.py and notebooks/01_example_imports.ipynb)

# Folder Structure
This template project folder structure is a modification from  cookiecutter datascience

 <pre>
/path/to/example/project/  
    ├── src/  
    │   ├── shared_code/   
    |   │   ├── __init__.py  
    │   │   └── shared_module.py      
    │   ├── __init__.py               
    │   └── project_specific_module.py      
    ├── test/  
    ├── notebooks/  
    ├── data/  
    ├── conf/  
    ├── pyproject.toml    
    ├── env_conda.yaml    
    ├── README.md                    
    └── setup.cfg  
</pre>

# Software Architecture
This repo-template is based on cookie-cutter data science template and aims at filling the gap to work in a infrastructure-awareless schema

The main issue when developing data science/engineering software is to tight relationship to data, from a infrasturcture point of view, software is also very dependent on actual infrastrutre implementations

This template decouples source code from the running infrastructure by providing a data abstraction layer, the general schema is:
1. Define data-assets with the metadata of a specific data asset
2. Each data asset is used to set data connector
3. Instantiate the data connection with the factory: `src/shared_code/connections.data_connections_factory`.

To define a new data connector kind: Implement a new data connector using inheritance. This data connector may need a new connector client, that can be implemented by inheritance

In addition, it is important to isolate the concept of dataset from its actual implementation, for example: `pandas.DataFrames`, `tensorflow.data.Datasets`, `numpy.ndarray`  etc. This allow to share and standardize common tasks.

## Shared 
Shared among projects, this package contains the actual implementation of the software architecture described above

* connections.py: Data connections abstractions and connections clients.
* dataset.py: Dataset Abstraction
* utils.py: Path definitions according to folder structure and general utils

## Project Specific
*Add project specific software architecture*

Project specific packages and modules are placed directly in `src/`

data_assets.py: Define environment variables, and a set of sources and sinks for your project

### Prerequisites
* Fill environment variables in .env file

### Data Assets
This project data assets (sources, sinks and intermediate assets for your project that are not considered
in common convention) are defined in `src/data_assets.py`

### Environment variables
This project environment variables retrieval and setting is implemented in `src/project_env.py`
This module will use dotenv to automatically retrieve environment variables' values from .env

# Naming Convention

## Shared

### Data Layers
Defined in `src/shared_code/utils.py`
* raw
* interim
* processed

### Data Assets
Defined in `src/shared_code/utils.py`. This set of DataAssets is expected enforce a folder/file naming convention,
so that it is expected to be shared among projects.

In a ML project, the following set is expected:
* dataset
* train
* features
* preds

## Project Specific
*Describe project specific naming conventions*

# Developments
This repo contains a sample pipeline, that gives an example of the implemented software architecture on notebooks

## Entry points
*Describe project's main processes and entry points*

## Notebooks

* 00_template: Pattern notebook
* 01_ex_imports
* 02_ex_connections
* 03_ex_pipeline

# References
## Methodology
* [Laszlo Sragner - Clean Architecture: How to Structure Your ML Projects to Reduce Technical Debt](https://www.youtube.com/watch?v=QXfsS-ZOeyA)
* [Cookiecutter Data Science](https://drivendata.github.io/cookiecutter-data-science/)

## Technical
* [Managing environments](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#activating-an-environment)
* [A Practical Guide to Setuptools and Pyproject.toml](https://godatadriven.com/blog/a-practical-guide-to-setuptools-and-pyproject-toml/)
* [How to Build a Complete Python Package Step-by-Step](https://www.youtube.com/watch?v=5KEObONUkik)
