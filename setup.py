import setuptools

# This setup file is required for Apache Beam/Dataflow templates
# to properly package and deploy dependencies to the worker machines.

setuptools.setup(
    name='dataflow_etl_pipeline',
    version='1.0.0',
    # Note: If we added non-standard libraries (like requests, numpy) they would go here:
    install_requires=[], 
    packages=setuptools.find_packages(),
    description='Apache Beam Dataflow ETL Pipeline for Transactions'
)
