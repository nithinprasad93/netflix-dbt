
-- Install Virtual Environment in your system
pip3 install virtualenv

-- Crete a virtual environment folder
virtualenv venv

-- Activate the virtual environment
source venv/bin/activate

--Install dbt-snowflake package
pip install dbt-snowflake==1.9.0

--Create a root directory for your dbt project
mkdir ~/.dbt

--initialize a new dbt project
dbt init netflix