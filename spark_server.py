#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import findspark
findspark.init()

from flask import Flask, jsonify, request
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StructType

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

#-----------------------------PATHS

datasetPath = "../dataset_donations/"
donationsPath = datasetPath + "Donations.csv"
donorsPath = datasetPath + "Donors.csv"
projectsPath = datasetPath + "Projects.csv"
resourcesPath = datasetPath + "Resources.csv"
schoolsPath = datasetPath + "Schools.csv"
teachersPath = datasetPath + "Teachers.csv"

#-------------LOADS ---------------
#o spark eh lazy por isso apenas faz as ops depois de uma accao. para nao estar a definir varias vezes o mesmo defino aqui uma vez

donations =spark.read.schema(schemaDonations).format("csv").options(header="true").load(donationsPath)
donors = spark.read.schema(schemaDonors).format("csv").options(header="true").load(donorsPath)
projects = spark.read.schema(schemaProjects).format("csv").options(header="true").load(projectsPath)
resources = spark.read.schema(schemaResources).format("csv").options(header="true").load(resourcesPath)
schools = spark.read.schema(schemaSchools).format("csv").options(header="true").load(schoolsPath)
teachers = spark.read.schema(schemaTeachers).format("csv").options(header="true").load(teachersPath)

@app.route('/donorschoose/projects/new', methods=['POST'])
def new_project():
    
    proj = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(32))
    school_id = request.args.get('school_id', None)
    teacher_id = request.args.get('teacher_id', None)
    proj_type = request.args.get('proj_type', None)
    proj_essay = request.args.get('proj_essay', None)
    proj_title = request.args.get('proj_title', None)
    proj_short_description = request.args.get('proj_short_description', None)
    proj_need_stat = request.args.get('proj_need_stat', None)
    proj_category = request.args.get('proj_category', None)
    proj_sub_categ = request.args.get('proj_sub_categ', None)
    proj_grade_level = request.args.get('proj_grade_level', None)
    proj_resource = request.args.get('proj_resource', None)
    proj_cost = request.args.get('proj_cost', None)
    proj_exp_date = request.args.get('proj_exp_date', None)
    proj_posted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
    
    return jsonify("new project created with success")

@app.route('/donorschoose/donations/new', methods=['POST'])
def new_donation():
    
    donation = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(32))
    donor = request.args.get('donor', None)
    donor_opt = request.args.get('donor_opt', None)
    amount = request.args.get('amount', None)
    cart_seq = request.args.get('cart_seq', None)
    project_id = request.args.get('project_id', None)
    date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")

    return jsonify("new donation concluded with success")

@app.route('/donorschoose/projects/', methods=['GET'])
def project_details():
    
    project_id = request.args.get('project_id', None)
    result = projects.filter(projects.Project_ID == project_id).head(10)
    answer = [] 
    for row in result:
        answer.append("Title: " + str(row.Project_Title))
        answer.append("School: " + str(row.School_ID))
        answer.append("Teacher: " + str(row.Teacher_ID))
        answer.append("Type: " + str(row.Project_Type))
        answer.append("Category: " + str(row.Project_Subject_Category_Tree))
        answer.append("Need: " + str(row.Project_Need_Statement))
        answer.append("Resource Category: " + str(row.Project_Resource_Category))
        answer.append("Grade Level: " + str(row.Project_Grade_Level_Category))
        answer.append("Cost: " + str(row.Project_Cost) + "$")
        answer.append("Posted on: " + str(row.Project_Posted_Date))
        answer.append("Expires on: " + str(row.Project_Expiration_Date))
        answer.append("Fully funded on: " +str( row.Project_Fully_Funded_Date))
        answer.append("Status: " + str(row.Project_Current_Status))
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
    projectos_professor = projects.filter(projects.Teacher_ID == teacher)\
    .select('Project_ID','Project_Title','Project_Cost','Project_Expiration_Date')
    
    lista_projectos = []
    for row in projectos_professor:
        lista_projectos.append(str(row.Project_ID))
    
    doacoes = donations.filter(donations.Project_ID.isin(lista_projectos))\
    .groupby('Project_ID')\
    .sum('Donation_Amount')
    
    result = projectos_professor.join(doacoes, projectos_professor.Project_ID == doacoes.Project_ID)
    
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

    answer = []

    
    return jsonify(answer)

@app.route('/donorschoose/teachers/', methods=['PUT'])
def update_teacher_prefix():
    
    teacher = request.args.get('teacher_id', None)
    prefix = request.args.get('prefix', None)
      
    return jsonify("Prefix updated with success")

@app.route('/', methods=['GET'])
def home():
    return jsonify("Bem-vindo ao donorschoose!")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)


