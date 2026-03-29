# clearspend_etl_pipeline

This is a Data warehouse for ClearSpend, a company which processes Debit and Credit transactions, an ETL pipeline is created to ensure proper data analysis with business insights. 

Setup instructions : 

The pipeline is created by connecting two programming environments Python (VS Code), and SQL (PostGres), the codes attached start from utmost beginning, i.e. connecting the environments, to the creation of data marts for the specific teams. To run the whole program, VS Code with python extentions, PGADMIN and PostGres needs to be installed on your device. Once installed, the python files must be runned in sequence, and the order is defined through the file names. The format is SequenceNumber_LayerName.py (For example : 06_transformation_mcc_data.py), accordingly, 01_server_connect.py is to be runned first, and following... 

Note that before running the code, you must input your PostGres Server details, and the datasets shall be downloaded on your device, and you should input your respective file path where asked. Some of the codes might take a longer time to run depending on your device due to the file sizes.

Link to the datasets : https://www.dropbox.com/scl/fi/jbksqcmsfd2erbpbqalff/Dataset-final-project.zip?rlkey=ftldt0rk5j4ofvxum1ggiwhn1&e=1&dl=0

Created by : 
Advani Vansh (i6348194), Brînzila Stefan (i6342386), Kaefer Ben (i6357052), Velenciuc Cristian (i6364905)

