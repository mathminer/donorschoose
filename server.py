#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from flask import Flask, jsonify, request
from google.cloud import bigquery
import datetime
import random
import string
import config

app = Flask(__name__)

bigclient=bigquery.Client()
dataset_id = "handy-zephyr-235119.donorschoose"
dataset_ref = bigclient.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)

projects_table = dataset_id+'.Projects'
donations_table = dataset_id+'.Donations'
schools_table = dataset_id+'.Schools'
teachers_table = dataset_id+'.Teachers'


@app.route('/test1')
def test1():
    return "Hello World!"

@app.route('/test2', methods=['GET'])
def test2():
    
    status = request.args.get('status', None)
    
    query = ('SELECT Project_Title, Project_ID FROM `' + projects_table +\
             '` LIMIT 3')
    
    query_job = bigclient.query(query)
    rows = query_job.result()
    
    answer = []
    for row in rows:
        answer.append(row.Project_ID + " - " + row.Project_Title)
    
    return jsonify(answer)




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
    
    schema = [
            bigquery.SchemaField('Project_ID','STRING',mode='NULLABLE'),
            bigquery.SchemaField('School_ID','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Teacher_ID','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Type','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Title','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Essay','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Short_Descripton','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Need_Statement','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Subject_Category_Tree','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Subject_Subcategory_Tree','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Resource_Category','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Cost','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Posted_Date','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Expiration_Date','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Project_Current_Status','STRING',mode='NULLABLE')
            ]
    
    row = [(proj,school_id,teacher_id,proj_type,proj_title,proj_essay,proj_short_description,proj_need_stat,proj_category,proj_sub_categ,proj_grade_level,proj_resource,proj_cost,proj_posted_date,proj_exp_date,'Live')]
    
    errors = bigclient.insert_rows(projects_table,row,selected_fields=schema)
    assert(errors == [])
    
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

    schema = [
            bigquery.SchemaField('Project_ID','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Donation_ID','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Donor_ID','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Donation_Included_Optional_Donation','BOOLEAN',mode='NULLABLE'),
            bigquery.SchemaField('Donation_Amount','FLOAT',mode='NULLABLE'),
            bigquery.SchemaField('Donor_Cart_Sequence','STRING',mode='NULLABLE'),
            bigquery.SchemaField('Donation_Received_Date','STRING',mode='NULLABLE'),
            ]
    
    row = [(project_id,donation,donor,donor_opt,amount,cart_seq,date)]
   
    errors = bigclient.insert_rows(donations_table,row,selected_fields=schema)
    assert(errors == [])
    
    return jsonify("new donation concluded with success")

@app.route('/donorschoose/projects/', methods=['GET'])
def porject_details():
    
    project_id = request.args.get('project_id', None)
    
    query = ('SELECT P.Project_Title, P.School_ID, P.Teacher_ID, P.Project_Type, P.Project_Need_Statement,\
             P.Project_Subject_Category_Tree, P.Project_Subject_Subcategory_Tree, P.Project_Grade_Level_Category,\
             P.Project_Resource_Category, P.Project_Cost, P.Project_Posted_Date, P.Project_Expiration_Date,\
             P.Project_Current_Status, P.Project_Fully_Funded_Date,\
             SUM(D.Donation_Amount) as Total_Donated \
          FROM `' + projects_table + '` as P, `' + donations_table + '` as D\
          WHERE P.Project_ID=D.Project_ID and P.Project_ID="' + project_id + '"\
          GROUP BY P.Project_Title, P.School_ID, P.Teacher_ID, P.Project_Title, P.Project_Type, P.Project_Need_Statement,\
             P.Project_Subject_Category_Tree, P.Project_Subject_Subcategory_Tree, P.Project_Grade_Level_Category,\
             P.Project_Resource_Category, P.Project_Cost, P.Project_Posted_Date, P.Project_Expiration_Date,\
             P.Project_Current_Status, P.Project_Fully_Funded_Date\
          LIMIT 100')
    
    query_job = bigclient.query(query)
    rows = query_job.result()
    
    answer = []
    for row in rows:
        answer.append("Title: " + row.Project_Title)
        answer.append("School: " + row.School_ID)
        answer.append("Teacher: " + row.Teacher_ID)
        answer.append("Type: " + row.Project_Type)
        answer.append("Category: " + row.Project_Subject_Category_Tree)
        answer.append("Subcategory: " + row.Project_Subject_Subcategory_Tree)
        answer.append("Need: " + row.Project_Need_Statement)
        answer.append("Resource Category: " + row.Project_Resource_Category)
        answer.append("Grade Level: " + row.Project_Grade_Level_Category)
        answer.append("Cost: " + str(row.Project_Cost) + "$")
        answer.append("Total Donated: " + str(row.Total_Donated) + "$")
        answer.append("Posted on: " + str(row.Project_Posted_Date))
        answer.append("Expires on: " + str(row.Project_Expiration_Date))
        answer.append("Fully funded on: " +str( row.Project_Fully_Funded_Date))
        answer.append("Status: " + row.Project_Current_Status)
        
    return jsonify(answer)

@app.route('/donorschoose/projects/findByDonor', methods=['GET'])
def find_by_donor():
    
    donor = request.args.get('donor', None)
    
    query=('SELECT Project_Title, Project_ID\
           FROM `' + projects_table + '` \
           WHERE Project_ID IN (\
           SELECT Project_ID FROM `'\
            + donations_table + '` \
            WHERE Donor_ID="' + donor + '") \
           LIMIT 100')
    
    query_job = bigclient.query(query)
    rows = query_job.result()
    
    answer = []
    for row in rows:
        answer.append(row.Project_ID + " - " + row.Project_Title)
        
    return jsonify(answer)


@app.route('/donorschoose/projects/findByStatus', methods=['GET'])
def find_by_status():
    
    status = request.args.get('status', None)
    
    query = ('SELECT Project_Title, Project_ID FROM `' + projects_table +\
             '` WHERE Project_Current_Status="' + status +\
            '" LIMIT 100')
    
    query_job = bigclient.query(query)
    rows = query_job.result()
    
    answer = []
    for row in rows:
        answer.append(row.Project_ID + " - " + row.Project_Title)
    
    return jsonify(answer)

@app.route('/donorschoose/projects/findByTeacher', methods=['GET'])
def find_by_teacher():
    
    teacher = request.args.get('teacher_id', None)
    
    query = ('SELECT P.Project_ID, P.Project_Title, P.Project_Cost, P.Project_Expiration_Date,\
          SUM(D.Donation_Amount) as Total_Donated \
          FROM `' + projects_table + '` as P, `' + donations_table + '` as D\
          WHERE P.Project_ID=D.Project_ID and Teacher_ID="' + teacher + '"\
          GROUP BY P.Project_ID, P.Project_Title, P.Project_Cost, P.Project_Expiration_Date\
          LIMIT 100')
    
    query_job = bigclient.query(query)
    rows = query_job.result()
    
    answer = []
    for row in rows:
        answer.append("Project: " + row.Project_ID + " - " + row.Project_Title)
        answer.append("Total Cost: " + str(row.Project_Cost) + "$")
        answer.append("Total Donated: " + str(row.Total_Donated) + "$")
        answer.append("Expires on: " + str(row.Project_Expiration_Date))
        answer.append("--------------------------")
    
    return jsonify(answer)

@app.route('/donorschoose/projects/findBySchool', methods=['GET'])
def find_by_school():
    
    school = request.args.get('school_id', None)
    
    query = ('SELECT Project_ID, Project_Title\
          FROM `' + projects_table + '` \
          WHERE School_ID="' + school + '"\
          LIMIT 100')
    
    query_job = bigclient.query(query)
    rows = query_job.result()
    
    answer = []
    for row in rows:
        answer.append(row.Project_ID + " - " + row.Project_Title)
    
    return jsonify(answer)

@app.route('/donorschoose/projects/findByNeed', methods=['GET'])
def find_by_need():
    
    state = request.args.get('state', None)

    query = ('SELECT \
                P.School_ID,\
                S.School_State, \
                S.School_Name, \
                P.Project_ID, \
                P.Project_Cost,\
                SUM(P.Project_Cost)  AS Soma_custo_projectos , \
                SUM(D.Donation_Amount) AS Total_Doacoes,\
                SUM(P.Project_Cost) - SUM(D.Donation_Amount) AS Diferenca \
                FROM `' + schools_table   + '` AS S,`' \
                       + projects_table  + '` AS P,`' \
                       + donations_table + '` AS D \
                WHERE \
                S.School_ID = P.School_ID AND \
                D.Project_ID = P.Project_ID AND \
                P.Project_Current_Status = "Live" AND \
                S.School_State = '+str(state)+' \
                GROUP BY \
                P.School_ID, \
                P.Project_Cost,\
                S.School_State, \
                P.Project_ID, \
                S.School_Name \
                ORDER BY Diferenca DESC \
                LIMIT 10;')
    
    query_job = bigclient.query(query)
    rows = query_job.result()
    
    answer = []
    for row in rows:
        answer.append("Project: " + row.Project_ID)
        answer.append("School: " + row.School_Name)
        answer.append("Total Cost: " + str(row.Soma_custo_projectos) + "$")
        answer.append("Missing: " + str(row.Diferenca) + "$")
        answer.append("-----------------------")
    
    return jsonify(answer)

@app.route('/donorschoose/teachers/', methods=['PUT'])
def update_teacher_prefix():
    
    teacher = request.args.get('teacher_id', None)
    prefix = request.args.get('prefix', None)
    
    query = ('UPDATE `' + teachers_table + '` \
             SET Teacher_Prefix="' + prefix + '" \
             WHERE Teacher_ID="' + teacher + '"')
    
    query_job = bigclient.query(query)
    rows = query_job.result()
    
    return jsonify("Prefix updated with success")

@app.route('/', methods=['GET'])
def home():
    return jsonify("Bem-vindo ao donorschoose!")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)
