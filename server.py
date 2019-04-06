#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from flask import Flask, jsonify, request
from multiprocessing import Value
from google.cloud import bigquery
import datetime
import random
import string

app = Flask(__name__)

bigclient=bigquery.Client()
dataset_id = "handy-zephyr-235119.donorschoose"
dataset_ref = bigclient.dataset(dataset_id)
dataset = bigquery.Dataset(dataset_ref)

projects_table = dataset_id+'.Projects'
donations_table = dataset_id+'.Donations'
schools_table = dataset_id+'.Schools'


@app.route('/donorschoose/projects/new', methods=['POST'])
def new_project():
    proj = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(32))
    school_id = request.args.get('school_id', None)
    teacher_id = request.args.get('teacher_id', None)
    teacher_proj_posted_seq = request.args.get('teacher_proj_posted_seq', None)
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
    
    query=('INSERT INTO '+ donations_table + ' (Project_ID, School_ID, Teacher_ID, Teacher_Project_Posted_Sequence, Project_Type,\
                                                Project_Title, Project_Essay, Project_Short_Descripton, Project_Need_Statement, \
                                                Project_Subject_Category_Tree, Project_Subject_Subcategory_Tree, Project_Grade_Level_Category, Project_Resource_Category, \
                                                Project_Cost, Project_Posted_Date, Project_Expiration_Date, Project_Current_Status)\
    VALUES (' + proj + ',' + school_id + ',' + teacher_id + ',' + teacher_proj_posted_seq + ',' + proj_type + ',' + proj_title +\
            ',' + proj_essay + ',' + proj_short_description + ',' + proj_need_stat + ',' + proj_category + ',' + proj_sub_categ +\
            ',' + proj_grade_level + + proj_resource + ',' + proj_cost + ',' + proj_posted_date + ',' + proj_exp_date + 'Live)')
    res = bigclient.query(query)
    return jsonify(res)

# http://127.0.0.1:5000/donorschoose/donations/new?donor=000210a2a948e929d8e04897dc921d91&donor_opt=true&amount=50&cart_seq=1&project_id=b8b63629e8c460ba90f64695cb3e0c30
@app.route('/donorschoose/donations/new', methods=['POST'])
def new_donation():
    donation = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(32))
    donor = request.args.get('donor', None)
    donor_opt = request.args.get('donor_opt', None)
    amount = request.args.get('amount', None)
    cart_seq = request.args.get('cart_seq', None)
    project_id = request.args.get('project_id', None)
    date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S %Z")
    '''
    query=('INSERT INTO '+ donations_table + ' (Donation_ID, Donor_ID, Project_ID, Donation_Included_Optional_Donation, Donor_Amount, Donor_Cart_Sequence, Donation_Received_Date) VALUES (' + donation + ',' + donor + ',' + project_id + ',' + donor_opt + ',' + amount + ',' + cart_seq + ',' + date + ')')
    res = bigclient.query(query)
    '''
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
    assert errors == []
    return jsonify("ok")


@app.route('/donorschoose/projects/findByDonor', methods=['GET'])
def find_by_donor():
    donor = request.args.get('donor', None)
    query=('SELECT Project_Title, Project_ID FROM `' + projects_table + '` WHERE Project_ID IN (SELECT Project_ID FROM `'\
            + donations_table + '` WHERE Donor_ID="'+donor+'") LIMIT 100')
    query_job = bigclient.query(query)
    rows = query_job.result()
    answer = []
    for row in rows:
        answer.append(row.Project_Title + " - " + row.Project_ID)
    return jsonify(answer)


@app.route('/donorschoose/projects/findByStatus', methods=['GET'])
def find_by_status():
    status = request.args.get('status', None)
    query=('SELECT Project_Title, Project_ID FROM `' + projects_table + '` WHERE Project_Current_Status="'+status+'" LIMIT 100')
    query_job = bigclient.query(query)
    rows = query_job.result()
    answer = []
    for row in rows:
        answer.append(row.Project_Title + " - " + row.Project_ID)
    return jsonify(answer)


@app.route('/donorschoose/projects/findByNeed', methods=['GET'])
def find_by_need():
    state = request.args.get('state', None)

    query = ('SELECT \
                S.School_State, \
                S.School_Name, \
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
                S.School_State = '+state+' \
                GROUP BY \
                P.School_ID, \
                S.School_State, \
                S.School_Name \
                ORDER BY Diferenca DESC \
                LIMIT 10;')
    
    query_job = bigclient.query(query)
    rows = query_job.result()
    answer = []
    for row in rows:
        answer.append(row.School_Name + " - " + str(row.Diferenca))
    return jsonify(answer)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=config.PORT, debug=config.DEBUG_MODE)
