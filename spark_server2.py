#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import random
import string
import datetime

#import findspark
#findspark.init()

from flask import Flask, jsonify, request

import os
from google.cloud import dataproc_v1, storage
from googleapiclient import discovery

app = Flask(__name__)

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
model_train_regression = 'jobs/model_linear_regression.py'
predictPercentagePath = 'jobs/predict_percentage.py'
model_train_clustering = 'jobs/model_clustering.py'
findSimilarPath = 'jobs/find_similar.py'

#---------------DATASET-------------

ML_PATH = 'gs://dados_donorschoose/ml/'
dataframe_file_name = ML_PATH + 'projetos_transformados'
model_file_name = ML_PATH + 'modelo_regressao'
model_cluster_file_name = ML_PATH + 'modelo_cluster'

#--------DADOS DO DATAPROC----------

project_id = 'handy-zephyr-235119'
cluster_name = 'dataprocdonorschoose'
region = 'global'
dataproc_cluster_client = dataproc_v1.ClusterControllerClient()
dataproc_job_client = dataproc_v1.JobControllerClient()
dataproc_cluster = dataproc_cluster_client.get_cluster(project_id,region,cluster_name)
dataproc_cluster_bucket = dataproc_cluster.config.config_bucket
storage_client = storage.Client()

@app.route('/donorschoose/projects/predictPercentage', methods=['GET'])
def find_similar():

  args = list()
  args.append(request.args.get('project', None))
  answer = process_request(predictPercentagePath,args)
  return jsonify(answer)

@app.route('/donorschoose/projects/findSimilar', methods=['GET'])
def predict_percentage():

  args = list()
  args.append(request.args.get('project', None))
  answer = process_request(findSimilarPath,args)
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

def train_models():
  '''bucket = storage_client.get_bucket(dataproc_cluster_bucket)
  if not storage.Blob(bucket=bucket, name=model_file_name).exists(storage_client):
    print('A criar modelo de regressao...')
    job_id = submit_pyspark_job(model_train_regression, [])
    wait_for_job(job_id)
    print('Modelo criado!')'''
  job_id = submit_pyspark_job(model_train_clustering, [])
  wait_for_job(job_id)

if __name__ == '__main__':
    #train_models()
    if not os.path.exists(results_path):
      os.mkdir(results_path)
    app.run(host="0.0.0.0", port=5000)
    


