#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import string
import datetime

import findspark
findspark.init()

from flask import Flask, jsonify, request

from pyspark.ml.feature import StringIndexer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext, types
from pyspark.sql.types import StructType, IntegerType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import isnan
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import Column
from pyspark.ml import Pipeline

#for k-means
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

app = Flask(__name__)

conf = SparkConf().setAppName('DonorschooseApp')
sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .appName("DonorschooseApp") \
    .getOrCreate()

sql_sc = SQLContext(sc)

## ----------------------------- SCHEMAS 

schemaDonations = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'Project_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donation_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donor_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donation_Included_Optional_Donation', 'nullable': True, 'type': 'boolean'},
  {'metadata': {}, 'name': 'Donation_Amount', 'nullable': True,'type': 'float'},
  {'metadata': {}, 'name': 'Donor_Cart_Sequence', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'Donation_Received_Date', 'nullable': True, 'type': 'timestamp'}],
 'type': 'struct'})

schemaDonors = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'Donor_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donor_City', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donor_State', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donor_Is_Teacher', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donor_Zip', 'nullable': True,'type': 'string'}],
 'type': 'struct'})

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

schemaResources = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'Project_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Resource_Item_Name', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Resource_Quantity', 'nullable': True, 'type': 'float'},
  {'metadata': {}, 'name': 'Resource_Unit_Price', 'nullable': True, 'type': 'float'},
  {'metadata': {}, 'name': 'Resource_Vendor_Name', 'nullable': True,'type': 'string'}],
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

schemaTeachers = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'Teacher_ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Teacher_Prefix', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Teacher_First_Project_Posted_Date', 'nullable': True, 'type': 'date'}],
 'type': 'struct'})

#--------------PATHS--------------

#LOCAL
#datasetPath = "../dataset_donations/"

#NA CLOUD
datasetPath = "gs://dados_donorschoose/"
donationsPath = datasetPath + "Donations.csv"
donorsPath = datasetPath + "Donors.csv"
projectsPath = datasetPath + "Projects.csv"
resourcesPath = datasetPath + "Resources.csv"
schoolsPath = datasetPath + "Schools.csv"
teachersPath = datasetPath + "Teachers.csv"



#-------------LOADS ---------------
#o spark eh lazy por isso apenas faz as ops depois de uma accao. para nao estar a definir varias vezes o mesmo defino aqui uma vez
'''
donations =spark.read.schema(schemaDonations).format("csv").options(header="true").load(donationsPath)
donors = spark.read.schema(schemaDonors).format("csv").options(header="true").load(donorsPath)
projects = spark.read.schema(schemaProjects).format("csv").options(header="true").load(projectsPath)
resources = spark.read.schema(schemaResources).format("csv").options(header="true").load(resourcesPath)
schools = spark.read.schema(schemaSchools).format("csv").options(header="true").load(schoolsPath)
teachers = spark.read.schema(schemaTeachers).format("csv").options(header="true").load(teachersPath)'''

donations =spark.read.csv(donationsPath)
donors = spark.read.csv(donorsPath)
projects = spark.read.csv(projectsPath)
resources = spark.read.csv(resourcesPath)
schools = spark.read.csv(schoolsPath)
teachers = spark.read.csv(teachersPath)

lr_model = 0
projetos_transformados = None
X = []


'''def K_means():
  knr = 2
  cols = ["Project_Subject_Category_Tree","Project_Subject_Subcategory_Tree","Project_Grade_Level_Category","Project_Resource_Category"]
  colsa = []
  
  df = projects.select(cols)
  
  df = df.where(df.Project_Subject_Category_Tree.isNotNull())
  df = df.where(df.Project_Subject_Subcategory_Tree.isNotNull())
  df = df.where(df.Project_Grade_Level_Category.isNotNull())
  df = df.where(df.Project_Resource_Category.isNotNull())
  for i in range(len(cols)):
  	stringIndexer = StringIndexer(inputCol=cols[i], outputCol=cols[i]+"a")
    model = stringIndexer.fit(df)
    df = model.transform(df)
    colsa.append(cols[i]+"a")

	
	
  for i in range(len(cols)):
	  encoder = OneHotEncoder(inputCol=cols[i]+"a", outputCol=cols[i]+"v")
	  encoded = encoder.transform(df)
	
		
  assembler = VectorAssembler(
  inputCols=colsa,
  outputCol="features")
  output = assembler.transform(encoded)

  output.show()

  # Trains a k-means model.
  kmeans = KMeans().setK(2).setSeed(1)
  model = kmeans.fit(output)
  # Evaluate clustering by computing Silhouette score
  # print(str(knr) + str(ClusteringEvaluator()));'''



def regressionModel():
  global projetos_transformados
  global lr_model
  global X

  projects2 = projects.withColumnRenamed('School_ID','School_ID_Projetos')
  projetos = projects2.filter((projects2.Project_Current_Status == 'Expired') | (projects2.Project_Current_Status == 'Fully Funded'))\
            .join(schools, projects2.School_ID_Projetos == schools.School_ID, how = 'left')

  
  lista_projectos = []
  for row in projetos.head(100):
    lista_projectos.append(str(row.Project_ID))

  doacoes = donations.filter(donations.Project_ID.isin(lista_projectos))\
    .groupby('Project_ID')\
    .sum('Donation_Amount').withColumnRenamed("sum(Donation_Amount)", "Sum_Donations")\
    .withColumnRenamed("Project_ID", "Proj_ID")

  projetos.createOrReplaceTempView('projetos')
  doacoes.createOrReplaceTempView('doacoes')

  query = ('SELECT * \
            FROM doacoes, projetos  \
            WHERE Project_ID = Proj_ID')

  projetos = spark.sql(query)

  projetos = projetos.withColumn('Has_Proj_Short_Desc', projetos.Project_Short_Description.isNotNull())
  projetos = projetos.withColumn('Has_Proj_Need_Stat', projetos.Project_Need_Statement.isNotNull())
  projetos = projetos.withColumn('Has_Proj_Subject_Category', projetos.Project_Subject_Category_Tree.isNotNull())
  projetos = projetos.withColumn('Has_Proj_Resource_Category', projetos.Project_Resource_Category.isNotNull())

  colNames = ['Project_Short_Description', 'Project_Need_Statement', 'Project_Subject_Category_Tree', 'Project_Resource_Category','Project_Type',\
    'Project_Grade_Level_Category','School_Metro_Type','School_State','School_City','School_District']
  projetos = projetos.na.fill("none", colNames)
 
  projetos = projetos.withColumn("Percentage_Funded", projetos.Sum_Donations/projetos.Project_Cost * 100)\
    .select('Project_ID',
       'Teacher_ID',
       'Project_Type',
       'Project_Grade_Level_Category',
       'Project_Cost',
       'Has_Proj_Short_Desc',
       'Has_Proj_Need_Stat',
       'Has_Proj_Subject_Category',
       'Has_Proj_Resource_Category',
       'School_Metro_Type',
       'School_Percentage_Free_Lunch',
       'School_State',
       'School_City',
       'School_District',
       'Percentage_Funded')

  X = ['Teacher_ID_index',
       'Project_Type_index',
       'Project_Grade_Level_Category_index',
       'Project_Cost',
       'Has_Proj_Short_Desc',
       'Has_Proj_Need_Stat',
       'Has_Proj_Subject_Category',
       'Has_Proj_Resource_Category',
       'School_Metro_Type_index',
       'School_Percentage_Free_Lunch',
       'School_State_index',
       'School_City_index',
       'School_District_index',
       'Percentage_Funded']
  
  indexers = [StringIndexer(inputCol=column, outputCol=column+"_index").fit(projetos) \
    for column in list(set(projetos.columns)-set(['Project_Cost','School_Percentage_Free_Lunch','Has_Proj_Short_Desc',\
      'Has_Proj_Need_Stat','Has_Proj_Subject_Category','Has_Proj_Resource_Category','Project_ID'])) ]

  pipeline = Pipeline(stages=indexers)
  projetos_transformados = pipeline.fit(projetos).transform(projetos).cache()

  vectorAssembler = VectorAssembler(inputCols = X, outputCol = 'features')
  vprojetos_df = vectorAssembler.transform(projetos_transformados)
  vprojetos_df = vprojetos_df.select(['features', 'Percentage_Funded'])
  
  lr = LinearRegression(featuresCol = 'features', labelCol='Percentage_Funded', maxIter=10, regParam=0.3, elasticNetParam=0.8)

  lr_model = lr.fit(vprojetos_df)

@app.route('/donorschoose/projects/predictPercentage', methods=['GET'])
def predict_percentage():
  project = request.args.get('project', None)
  
  projecto_resultado = projetos_transformados.filter(projetos_transformados.Project_ID == project)
  
  vectorAssembler = VectorAssembler(inputCols = X, outputCol = 'features')
  vprojetos_df = vectorAssembler.transform(projecto_resultado)
  vprojetos_df = vprojetos_df.select(['features', 'Percentage_Funded'])

  predictions = lr_model.transform(vprojetos_df)
  result = predictions.select("prediction","Percentage_Funded",'features').head(1)

  answer = []
  for row in result:
       answer.append('Prediction for project ' + project + ':' + str(row.prediction))
  return jsonify(answer)

@app.route('/donorschoose/projects/findByDonor', methods=['GET'])
def find_by_donor():
    
    donor = request.args.get('donor', None)
   
    projectos_doados = donations.filter(donations.Donor_ID == donor)\
    .select("Project_ID")\
    .head(10)
    identificadores = []
    
    for row in projectos_doados:
       identificadores.append(str(row.Project_ID))
    
    result = projects.filter(projects.Project_ID.isin(identificadores))\
    .select("Project_ID","Project_Title")\
    .head(10)

    answer = []
    for row in result:
       answer.append(str(row.Project_ID) + " - " + str(row.Project_Title))
     
    return jsonify(answer)

@app.route('/donorschoose/projects/findByStatus', methods=['GET'])
def find_by_status():
    
    status = request.args.get('status', None)
    result = projects.filter(projects.Project_Current_Status == status)\
    .select("Project_ID","Project_Title")\
    .head(100)


    answer = [] 
    for row in result:
        answer.append(str(row.Project_ID) + " - " + str(row.Project_Title))
    
    return jsonify(answer)

@app.route('/donorschoose/projects/findByTeacher', methods=['GET'])
def find_by_teacher():
    
    teacher = request.args.get('teacher_id', None)
    
    projects.createOrReplaceTempView('projects')
    donations.createOrReplaceTempView('donations')
    
    '''projectos_professor = projects.filter(projects.Teacher_ID == teacher)\
    .select('Project_ID','Project_Title','Project_Cost','Project_Expiration_Date')
    
    lista_projectos = []
    for row in projectos_professor:
        lista_projectos.append(str(row.Project_ID))
    
    doacoes = donations.filter(donations.Project_ID.isin(lista_projectos))\
    .groupby('Project_ID')\
    .sum('Donation_Amount')
    
    result = projectos_professor.join(doacoes, projectos_professor.Project_ID == doacoes.Project_ID).show()'''
    
    query = ('SELECT P.Project_ID, P.Project_Title, P.Project_Cost, P.Project_Expiration_Date,\
          SUM(D.Donation_Amount) as Total_Donated \
          FROM projects  as P, donations as D\
          WHERE P.Project_ID=D.Project_ID and Teacher_ID="' + teacher + '"\
          GROUP BY P.Project_ID, P.Project_Title, P.Project_Cost, P.Project_Expiration_Date')
    
    result=spark.sql(query).head(100)
    
    answer = []
    for row in result:
        answer.append("Project: " + str(row.Project_ID) + " - " + str(row.Project_Title))
        answer.append("Total Cost: " + str(row.Project_Cost) + "$")
        answer.append("Total Donated: " + str(row.Total_Donated) + "$")
        answer.append("Expires on: " + str(row.Project_Expiration_Date))
        answer.append("--------------------------")
    return jsonify(answer)

@app.route('/donorschoose/projects/findBySchool', methods=['GET'])
def find_by_school():
    
    school = request.args.get('school_id', None)
    
    result = projects.filter(projects.School_ID == school).select('Project_ID','Project_Title').head(100)
    
    answer = []
    for row in result:
        answer.append(str(row.Project_ID) + " - " + str(row.Project_Title))
        
    return jsonify(answer)

@app.route('/donorschoose/projects/findByNeed', methods=['GET'])
def find_by_need():
    
    state = request.args.get('state', None)
    
    projects.createOrReplaceTempView('projects')
    schools.createOrReplaceTempView('schools')
    donations.createOrReplaceTempView('donations')

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

    result = spark.sql(query).head(100)
    
    answer = []
    for row in result:
        answer.append("Project: " + str(row.Project_ID))
        answer.append("School: " + str(row.School_Name))
        answer.append("Total Cost: " + str(row.Soma_custo_projectos) + "$")
        answer.append("Missing: " + str(row.Diferenca) + "$")
        answer.append("-----------------------")
    
    return jsonify(answer)

@app.route('/', methods=['GET'])
def home():
    return jsonify("Bem-vindo ao donorschoose!")

if __name__ == '__main__':
    regressionModel()
    app.run(host="0.0.0.0", port=5000)
    


