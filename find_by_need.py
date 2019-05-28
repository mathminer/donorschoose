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

projectsPath = datasetPath + "Projects.csv"
schoolsPath = datasetPath + "Schools.csv"
donationsPath = datasetPath + "Donations.csv"

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

schemaSchools = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'School_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School_Name', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School_Metro_Type', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School_Percentage_Free_Lunch', 'nullable': True,'type': 'integer'},
  {'metadata': {}, 'name': 'School_State', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School_Zip', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'School_City', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School_County', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School_District', 'nullable': True, 'type': 'string'}],
 'type': 'struct'})

projects = spark.read.schema(schemaProjects).format("csv").options(header="true").load(projectsPath)
schools = spark.read.schema(schemaSchools).format("csv").options(header="true").load(schoolsPath)
donations = spark.read.schema(schemaDonations).format("csv").options(header="true").load(donationsPath)

projects.createOrReplaceTempView('projects')
schools.createOrReplaceTempView('schools')
donations.createOrReplaceTempView('donations')

state = sys.argv[1]

query = ('SELECT \
                projects.School_ID,\
                schools.School_State, \
                schools.School_Name, \
                projects.Project_ID, \
                projects.Project_Cost,\
                SUM(projects.Project_Cost)  AS Soma_custo_projectos , \
                SUM(donations.Donation_Amount) AS Total_Doacoes,\
                SUM(projects.Project_Cost) - SUM(donations.Donation_Amount) AS Diferenca \
                FROM schools, projects, donations  \
              WHERE \
                schools.School_ID = projects.School_ID AND \
                donations.Project_ID = projects.Project_ID AND \
                projects.Project_Current_Status = "Live" AND \
                schools.School_State = ' + str(state) + ' \
              GROUP BY \
                projects.School_ID, \
                projects.Project_Cost,\
                schools.School_State, \
                projects.Project_ID, \
                schools.School_Name \
              ORDER BY Diferenca DESC')
print(init_str)
spark.sql(query).select('Project_ID','School_Name','Soma_custo_projectos','Diferenca').show()
print(end_str)