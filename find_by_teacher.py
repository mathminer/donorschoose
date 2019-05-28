
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.sql import SparkSession, SQLContext, types

import pyspark
import sys
import string

init_str = '----------//iniciodoresultado//'
end_str = '----------//fimdoresultado//'

conf = SparkConf().setAppName('DonorschooseApp')
sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .appName("DonorschooseApp") \
    .getOrCreate()

datasetPath = "gs://dados_donorschoose/dataset/"

donationsPath = datasetPath + "Donations.csv"
projectsPath = datasetPath + "Projects.csv"

schemaProjects = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'Project_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Teacher_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Teacher_Project Posted Sequence', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'Project_Type', 'nullable': True,'type': 'string'},
  {'metadata': {}, 'name': 'Project_Title', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Project_Essay', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Project_Short_Description', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Project_Need_Statement', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Project_Subject_Category_Tree', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Project_Subject_Subcategory_Tree','nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Project_Grade_Level_Category', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Project_Resource_Category', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Project_Cost', 'nullable': True, 'type': 'float'},
  {'metadata': {}, 'name': 'Project_Posted_Date', 'nullable': True, 'type': 'date'},
  {'metadata': {}, 'name': 'Project_Expiration_Date', 'nullable': True, 'type': 'date'},
  {'metadata': {}, 'name': 'Project_Current_Status', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Project_Fully_Funded_Date', 'nullable': True, 'type': 'date'}],
 'type': 'struct'})

schemaDonations = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'Project_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donation_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donor_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donation_Included_Optional_Donation', 'nullable': True, 'type': 'boolean'},
  {'metadata': {}, 'name': 'Donation_Amount', 'nullable': True,'type': 'float'},
  {'metadata': {}, 'name': 'Donor_Cart_Sequence', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'Donation_Received_Date', 'nullable': True, 'type': 'timestamp'}],
 'type': 'struct'})
 
projects = spark.read.schema(schemaProjects).format("csv").options(header="true").load(projectsPath)
donations =spark.read.schema(schemaDonations).format("csv").options(header="true").load(donationsPath)

projects.createOrReplaceTempView('projects')
donations.createOrReplaceTempView('donations')

teacher = sys.argv[1]
           
query = ('SELECT P.Project_ID, P.Project_Title, P.Project_Cost, P.Project_Expiration_Date,\
    SUM(D.Donation_Amount) as Total_Donated \
    FROM projects  as P, donations as D\
    WHERE P.Project_ID=D.Project_ID and Teacher_ID="' + teacher + '"\
    GROUP BY P.Project_ID, P.Project_Title, P.Project_Cost, P.Project_Expiration_Date')
    
print(init_str)
spark.sql(query).select('Project_ID','Project_Title','Project_Cost','Total_Donated','Project_Expiration_Date').show()
print(end_str)