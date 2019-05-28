#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import string
import datetime

import findspark
findspark.init()

from flask import Flask, jsonify, request

import os
from google.cloud import dataproc_v1, storage
from googleapiclient import discovery
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

app = Flask(__name__)

'''
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
 'type': 'struct'})'''
#---Processamento-de-Resultados---

init_str = '----------//iniciodoresultado//\n'
end_str = '----------//fimdoresultado//\n'

#--------------PATHS--------------

#LOCAL
datasetPath = "../dataset_donations/"

#NA CLOUD
bucket_name = 'dados_donorschoose'
datasetPath = bucket_name + 'dataset/'

#diretoria dos resultados spark
results_path = 'output'

#caminhos para os jobs
findByStausPath = 'jobs/find_by_status.py'
findByDonorPath = 'jobs/find_by_donor.py'
findByTeacherPath = 'jobs/find_by_teacher.py'
findBySchoolPath = 'jobs/find_by_school.py'
findByNeedPath = 'jobs/find_by_need.py'

#---------------DATASET-------------

donationsPath = datasetPath + "Donations.csv"
donorsPath = datasetPath + "Donors.csv"
projectsPath = datasetPath + "Projects.csv"
resourcesPath = datasetPath + "Resources.csv"
schoolsPath = datasetPath + "Schools.csv"
teachersPath = datasetPath + "Teachers.csv"

#--------DADOS DO DATAPROC----------

project_id = 'handy-zephyr-235119'
cluster_name = 'dataprocdonorschoose'
region = 'global'
dataproc_cluster_client = dataproc_v1.ClusterControllerClient()
dataproc_job_client = dataproc_v1.JobControllerClient()
dataproc_cluster = dataproc_cluster_client.get_cluster(project_id,region,cluster_name)
dataproc_cluster_bucket = dataproc_cluster.config.config_bucket
storage_client = storage.Client()


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

    args = list()
    args.append(request.args.get('donor', None))
    answer = process_request(findByDonorPath,args)
     
    return jsonify(answer)

@app.route('/donorschoose/projects/findByStatus', methods=['GET'])
def find_by_status():

    args = list()
    args.append(request.args.get('status', None))
    answer = process_request(findByStausPath,args)
    return jsonify(answer)

@app.route('/donorschoose/projects/findByTeacher', methods=['GET'])
def find_by_teacher():
    
    args = list()
    args.append(request.args.get('teacher_id', None))
    answer = process_request(findByTeacherPath,args)
    
    return jsonify(answer)

@app.route('/donorschoose/projects/findBySchool', methods=['GET'])
def find_by_school():

    args = list()
    args.append(request.args.get('school_id', None))
    answer = process_request(findBySchoolPath,args)
        
    return jsonify(answer)

@app.route('/donorschoose/projects/findByNeed', methods=['GET'])
def find_by_need():
    
    args = list()
    args.append(request.args.get('state', None))
    answer = process_request(findByNeedPath,args)

    return jsonify(answer)

@app.route('/', methods=['GET'])
def home():
    return jsonify("Bem-vindo ao donorschoose!")

def wait_for_job(job_id):
    """Wait for job to complete or error out."""
    print('Waiting for job to finish...')
    while True:
        job = dataproc_job_client.get_job(project_id, region, job_id)
        # Handle exceptions
        if job.status.State.Name(job.status.state) == 'ERROR':
            raise Exception(job.status.details)
        elif job.status.State.Name(job.status.state) == 'DONE':
            print('Job finished.')
            return job

def submit_pyspark_job(filename, args):
    """Submits the Pyspark job to the cluster, assuming `filename` has
    already been uploaded to `bucket_name`"""
    job_details = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': 'gs://{}/{}'.format(bucket_name, filename),
            'args': args
        }
    }
    result = dataproc_job_client.submit_job(
        project_id=project_id,
        region=region,
        job=job_details)
    job_id = result.reference.job_id
    print('Submitted job ID {}.'.format(job_id))
    return job_id

def download_blob(source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    bucket = storage_client.get_bucket(dataproc_cluster_bucket)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(results_path+'/'+destination_file_name)

    return results_path+'/'+destination_file_name 

def format_download_string(source_link):
  result = source_link.replace('gs://'+dataproc_cluster_bucket+'/','')
  return result + '.000000000'

def process_result_file(file_path):
  print(file_path)
  f = open(file_path,'r')
  result = list()
  lines = f.readlines()
  i=0
  while lines[i] != init_str:
    i+=1
  i+=1
  while lines[i] != end_str:
    result.append(lines[i])
    i+=1
  f.close()
  return result

def process_request(job_filepath, args):
  job_id = submit_pyspark_job(job_filepath, args)
  job = wait_for_job(job_id)
  download_string = format_download_string(job.driver_output_resource_uri)
  result_file = download_blob(download_string, job_id)
  return process_result_file(result_file)

if __name__ == '__main__':
    #regressionModel()
    if not os.path.exists(results_path):
      os.mkdir(results_path)
    app.run(host="0.0.0.0", port=5000)
    


