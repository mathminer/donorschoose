#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import findspark
findspark.init()

from flask import Flask, jsonify, request
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType

app = Flask(__name__)

conf = SparkConf().setAppName('DonorschooseApp')
sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .appName("DonorschooseApp") \
    .getOrCreate()

sql_sc = SQLContext(sc)

datasetPath = "../dataset_donations/"

schemaDonations = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'Project ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donation ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donor ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donation Included Optional Donation', 'nullable': True, 'type': 'boolean'},
  {'metadata': {}, 'name': 'Donation Amount', 'nullable': True,'type': 'float'},
  {'metadata': {}, 'name': 'Donor Cart Sequence', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'Donation Received Date', 'nullable': True, 'type': 'timestamp'}],
 'type': 'struct'})

donationsPath = datasetPath + "Donations.csv"

schemaDonors = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'Donor ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donor City', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donor State', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donor Is Teacher', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Donor Zip', 'nullable': True,'type': 'string'}],
 'type': 'struct'})

donorsPath = datasetPath + "Donors.csv"

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

projectsPath = datasetPath + "Projects.csv"

schemaResources = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'Project ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Resource Item Name', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Resource Quantity', 'nullable': True, 'type': 'float'},
  {'metadata': {}, 'name': 'Resource Unit_Price', 'nullable': True, 'type': 'float'},
  {'metadata': {}, 'name': 'Resource Vendor Name', 'nullable': True,'type': 'string'}],
 'type': 'struct'})

resourcesPath = datasetPath + "Resources.csv"

schemaSchools = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'School ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School Name', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School Metro Type', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School Percentage Free Lunch', 'nullable': True,'type': 'integer'},
  {'metadata': {}, 'name': 'School State', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School Zip', 'nullable': True, 'type': 'integer'},
  {'metadata': {}, 'name': 'School City', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School County', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'School District', 'nullable': True, 'type': 'string'}],
 'type': 'struct'})

schoolsPath = datasetPath + "Schools.csv"

schemaTeachers = StructType.fromJson({'fields': [
  {'metadata': {}, 'name': 'Teacher ID', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Teacher Prefix', 'nullable': True, 'type': 'string'},
  {'metadata': {}, 'name': 'Teacher First Project Posted Date', 'nullable': True, 'type': 'date'}],
 'type': 'struct'})

teachersPath = datasetPath + "Teachers.csv"

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
def porject_details():
    
    project_id = request.args.get('project_id', None)
    df = spark.read.schema(schemaProjects).format("csv").options(header="true").load(projectsPath)
    result = df.filter(df.Project_ID == project_id).head(10)
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
            
    answer = []  
    return jsonify(answer)


@app.route('/donorschoose/projects/findByStatus', methods=['GET'])
def find_by_status():
    
    status = request.args.get('status', None)
    

    answer = []    
    return jsonify(answer)

@app.route('/donorschoose/projects/findByTeacher', methods=['GET'])
def find_by_teacher():
    
    teacher = request.args.get('teacher_id', None)
        
    answer = []
    return jsonify(answer)

@app.route('/donorschoose/projects/findBySchool', methods=['GET'])
def find_by_school():
    
    school = request.args.get('school_id', None)
    
    answer = []

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


