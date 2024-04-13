import json
import os
import pandas as pd
import plotly.express as px
import mysql.connector
from sqlalchemy import create_engine
import streamlit as st
from streamlit_option_menu import option_menu
import requests
import numpy as np
import plotly.figure_factory as ff
import psycopg2

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="123456",
    database="phonepe",
    port="5432")

cursor = mydb.cursor()
cursor.execute("SELECT * FROM agg_tran")
mydb.commit()
table1 = cursor.fetchall()
columns = ["states", "years", "quarter", "transactiont_type", "transaction_count", "transaction_amount"]
agg_tran= pd.DataFrame(table1, columns=columns)


cursor = mydb.cursor()
cursor.execute("SELECT * FROM agg_user")
mydb.commit()
table2 = cursor.fetchall()
columns = ["states", "years", "quarter", "brands", "transaction_count", "transaction_percentage"]
agg_user = pd.DataFrame(table2, columns=columns)


cursor = mydb.cursor()
cursor.execute("SELECT * FROM map_tran")
mydb.commit()
table3 = cursor.fetchall()
columns = ["states", "years", "quarter", "districts", "transaction_count", "transaction_amount",]
map_tran = pd.DataFrame(table3, columns=columns)


cursor = mydb.cursor()
cursor.execute("SELECT * FROM map_user")
mydb.commit()
table4 = cursor.fetchall()
columns = ["states", "years", "quarter", "districts", "registered_users", "app_opens",]
map_user = pd.DataFrame(table4, columns=columns)


cursor = mydb.cursor()
cursor.execute("SELECT * FROM top_tran")
mydb.commit()
table5 = cursor.fetchall()
columns = ["states", "years", "quarter", "pincodes","districts", "transaction_count", "transaction_amount",]
top_tran = pd.DataFrame(table5, columns=columns)


cursor = mydb.cursor()
cursor.execute("SELECT * FROM top_user")
mydb.commit()
table6 = cursor.fetchall()
columns = ["states", "years", "quarter","pincodes", "registered_users"]
top_user = pd.DataFrame(table6, columns=columns)


def tran_transaction_amount_year(option, year):

    agtr = option[option["years"] == year]
    agtr.reset_index(drop=True, inplace=True)

    agtrg = agtr.groupby("states")[["transaction_count", "transaction_amount"]].sum()
    agtrg.reset_index(inplace=True)

    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)

    coll1, coll2 = st.columns(2)

    with coll1:
        fig_transaction_amount = px.bar(agtrg, x="states", y="transaction_amount", title=f"{year} transaction_amount",
                            color="transaction_amount", color_continuous_scale="ylgnbu",
                            range_color=(agtrg["transaction_amount"].min(), agtrg["transaction_amount"].max()), height=650, width=600)
        #st.plotly_chart(fig_transaction_amount)

        fig_ind_1 = px.choropleth(agtrg, geojson=data1, locations="states", featureidkey="properties.ST_NM",
                                  color="transaction_amount", color_continuous_scale="ylgnbu",
                                  range_color=(agtrg["transaction_amount"].min(), agtrg["transaction_amount"].max()), hover_name="states",
                                  title=f"{year}", fitbounds="locations", height=650, width=600)
        fig_ind_1.update_geos(visible=False)
        #st.plotly_chart(fig_ind_1)

    with coll2:
        fig_transaction_count = px.bar(agtrg, x="states", y="transaction_count", title=f"{year} transaction_count",
                           color="transaction_amount", color_continuous_scale="tempo",
                           range_color=(agtrg["transaction_amount"].min(), agtrg["transaction_amount"].max()), height=650, width=600)
        #st.plotly_chart(fig_transaction_count)

        fig_ind_2 = px.choropleth(agtrg, geojson=data1, locations="states", featureidkey="properties.ST_NM",
                                  color="transaction_count", color_continuous_scale="tempo",
                                  range_color=(agtrg["transaction_count"].min(), agtrg["transaction_count"].max()), hover_name="states",
                                  title=f"{year}", fitbounds="locations", height=650, width=600)
        fig_ind_2.update_geos(visible=False)
        #st.plotly_chart(fig_ind_2)

    return agtr


def tran_transaction_amount_year_quarter(option1, quarter):
    agtr = option1[option1["quarter"] == quarter]
    agtr.reset_index(drop=True, inplace=True)

    agtrg = agtr.groupby("states")[["transaction_count", "transaction_amount"]].sum()
    agtrg.reset_index(inplace=True)

    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)


    coll3, coll4 = st.columns(2)

    with coll3:
        fig_transaction_amount = px.bar(agtrg, x="states", y="transaction_amount", title=f"{agtr['years'].min()} Year {quarter} quarter transaction_amount",
                            color="transaction_amount", color_continuous_scale="ylgnbu",
                            range_color=(agtrg["transaction_amount"].min(), agtrg["transaction_amount"].max()), height=650, width=600)
        st.plotly_chart(fig_transaction_amount)

        fig_ind_1 = px.choropleth(agtrg, geojson=data1, locations="states", featureidkey="properties.ST_NM",
                                  color="transaction_amount", color_continuous_scale="ylgnbu",
                                  range_color=(agtrg["transaction_amount"].min(), agtrg["transaction_amount"].max()), hover_name="states",
                                  title=f"{agtr['years'].min()} Year {quarter} quarter transaction_amount", fitbounds="locations", height=650, width=600)
        fig_ind_1.update_geos(visible=False)
        st.plotly_chart(fig_ind_1)

    with coll4:
        fig_transaction_count = px.bar(agtrg, x="states", y="transaction_count", title=f"{agtr['years'].min()} Year {quarter} quarter transaction_count",
                           color="transaction_amount", color_continuous_scale="tempo",
                           range_color=(agtrg["transaction_amount"].min(), agtrg["transaction_amount"].max()), height=650, width=600)
        st.plotly_chart(fig_transaction_count)

        fig_ind_2 = px.choropleth(agtrg, geojson=data1, locations="states", featureidkey="properties.ST_NM",
                                  color="transaction_count", color_continuous_scale="tempo",
                                  range_color=(agtrg["transaction_count"].min(), agtrg["transaction_count"].max()), hover_name="states",
                                  title=f"{agtr['years'].min()} Year {quarter} quarter transaction_count", fitbounds="locations", height=650, width=600)
        fig_ind_2.update_geos(visible=False)
        st.plotly_chart(fig_ind_2)

    return agtr


def transaction_type(df, states):
    agtrg = df[df["states"]==(states)]
    agtrg.reset_index(drop=True, inplace=True) 

    coll3, coll4 = st.columns(2)
    with coll3:
        fig_pie1 = px.pie(data_frame=agtrg, names="transactiont_type", values="transaction_amount",
                      width=650, title=f"{states} {agtrg['years'].min()} - {quarter} quarter transaction_amount", hole=0.40)
        st.plotly_chart(fig_pie1)

    with coll4:
        fig_pie2 = px.pie(data_frame=agtrg, names="transactiont_type", values="transaction_count",
                        width=650, title=f"{states} {agtrg['years'].min()} - {quarter} quarter transaction_count", hole=0.40)
        st.plotly_chart(fig_pie2)

    return agtrg


def brands(df, year):
    agusy = df[df["years"] == year]
    agusy.reset_index(drop=True, inplace=True)

    agusyg = agusy.groupby("brands")["transaction_count"].sum()
    agusyg = agusyg.reset_index()

    colors = px.colors.qualitative.Plotly[:len(agusyg)]

    fig_bar = px.bar(data_frame=agusyg, x="brands", y="transaction_count", hover_name="brands",
                     width=980, height=600, text_auto='.3s', title=f"{agusy['years'].min()} brands transaction_count",
                     color=agusyg["brands"], color_discrete_sequence=colors)
  
    #fig_bar.show()

    return agusy

def brandsqu(df,quarter):
    agusq = df[df["quarter"] == quarter]
    agusq.reset_index(drop=True, inplace=True)

    agusqg = pd.DataFrame(agusq.groupby("brands")["transaction_count"].sum())
    agusqg = agusqg.reset_index()

    fig_bar = px.bar(data_frame=agusqg, x="brands", y="transaction_count", hover_name="brands",
                     width=980, height=600, text_auto='.3s', title=f"All Over India's -{year}-{quarter} quarter brands wise transaction_count",
                     color=agusqg["brands"])
    
    st.plotly_chart(fig_bar)

    return agusq


def brandstates(df, states, year):
    states_df = df[(df["states"] == states) & (df["years"] == year)]
    states_df.reset_index(drop=True, inplace=True)
    
    fig = px.sunburst(states_df, path=['states','brands','transaction_count'],title=f"{states}-brands wise transaction_count",
                      hover_name="brands", values='transaction_count')    
    
    st.plotly_chart(fig)

    return states_df

def mapdisttr(df,states):
    agusst = df[df["states"] == states]
    agusst.reset_index(drop=True, inplace=True)

    agusstg = agusst.groupby("districts")[["transaction_count","transaction_amount"]].sum()
    agusstg = agusstg.reset_index()
    
    colors = px.colors.qualitative.Plotly[:len(agusst)]

    fig_bar = px.bar(data_frame=agusstg, x="districts", y="transaction_count", hover_name="districts",
                    width=980, height=600,text_auto='.3s', title=f"{states} quarter districts Wise transaction_count",
                    color="districts", color_discrete_sequence=colors)

    fig_bar1 = px.bar(data_frame=agusstg, x="districts", y="transaction_amount", hover_name="districts",
                width=980, height=600,text_auto='.3s', title=f"{states} districts Wise transaction_amount",
                color="districts", color_discrete_sequence=colors)

    st.plotly_chart(fig_bar)
    st.plotly_chart(fig_bar1)
    return agusst

def mapdistus(df, states):
    agusst = df[df["states"] == states]
    agusst.reset_index(drop=True, inplace=True)

    agusstg = agusst.groupby("districts")[["registered_users", "app_opens"]].sum()
    agusstg = agusstg.reset_index()
    
    fig = px.sunburst(agusstg, values="app_opens", path=["districts","registered_users","app_opens"],hover_data="registered_users",
                      hover_name="districts",width=980, height=600)
    
    st.plotly_chart(fig)
    return agusst 
    

def top_tran_transaction_amount_year(option, year):

    toptry = option[option["years"] == year]
    toptry.reset_index(drop=True, inplace=True)

    toptryqg = toptry.groupby("states")[["transaction_count", "transaction_amount"]].sum()
    toptryqg.reset_index(inplace=True)

    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)

    coll3, coll4 = st.columns(2)

    with coll3:
        fig_transaction_amount = px.bar(toptryqg, x="states", y="transaction_amount", title=f"{year} transaction_amount",
                                color="transaction_amount", color_continuous_scale="ylgnbu",
                                range_color=(toptryqg["transaction_amount"].min(), toptryqg["transaction_amount"].max()), height=650, width=600)
        #st.plotly_chart(fig_transaction_amount)

        fig_ind_1 = px.choropleth(toptryqg, geojson=data1, locations="states", featureidkey="properties.ST_NM",
                                    color="transaction_amount", color_continuous_scale="ylgnbu",
                                    range_color=(toptryqg["transaction_amount"].min(), toptryqg["transaction_amount"].max()), hover_name="states",
                                    title=f"{year}", fitbounds="locations", height=650, width=600)
        fig_ind_1.update_geos(visible=False)
        #st.plotly_chart(fig_ind_1)

    with coll4:
        fig_transaction_count = px.bar(toptryqg, x="states", y="transaction_count", title=f"{year} transaction_count",
                            color="transaction_amount", color_continuous_scale="tempo",
                            range_color=(toptryqg["transaction_amount"].min(), toptryqg["transaction_amount"].max()), height=650, width=600)
        #st.plotly_chart(fig_transaction_count)

        fig_ind_2 = px.choropleth(toptryqg, geojson=data1, locations="states", featureidkey="properties.ST_NM",
                                    color="transaction_count", color_continuous_scale="tempo",
                                    range_color=(toptryqg["transaction_count"].min(), toptryqg["transaction_count"].max()), hover_name="states",
                                    title=f"{year}", fitbounds="locations", height=650, width=600)
        fig_ind_2.update_geos(visible=False)
        #st.plotly_chart(fig_ind_2)
    return toptry


def top_tran_transaction_amount_year_quarter(option1, quarter):

    toptryq = option1[option1["quarter"] == quarter]
    toptryq.reset_index(drop=True, inplace=True)

    toptryqg = toptryq.groupby("states")[["transaction_count", "transaction_amount"]].sum()
    toptryqg.reset_index(inplace=True)

    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)

    coll3, coll4 = st.columns(2)

    with coll3:
        fig_transaction_amount = px.bar(toptryqg, x="states", y="transaction_amount", title=f"{quarter}-quarter transaction_amount",
                                color="transaction_amount", color_continuous_scale="ylgnbu",
                                range_color=(toptryqg["transaction_amount"].min(), toptryqg["transaction_amount"].max()),height=600, width=700)
        st.plotly_chart(fig_transaction_amount)
        
        fig_ind_1 = px.choropleth(toptryqg, geojson=data1, locations="states", featureidkey="properties.ST_NM",
                                    color="transaction_amount", color_continuous_scale="ylgnbu",
                                    range_color=(toptryqg["transaction_amount"].min(), toptryqg["transaction_amount"].max()), hover_name="states",
                                    title=f"{quarter}", fitbounds="locations",height=600, width=700)
        fig_ind_1.update_geos(visible=False)
        st.plotly_chart(fig_ind_1)

    with coll4:

        fig_transaction_count = px.bar(toptryqg, x="states", y="transaction_count", title=f"{quarter}-quarter transaction_count",
                            color="transaction_count", color_continuous_scale="tempo",
                            range_color=(toptryqg["transaction_amount"].min(), toptryqg["transaction_amount"].max()), height=600, width=700)
        
        st.plotly_chart(fig_transaction_count)


        fig_ind_2 = px.choropleth(toptryqg, geojson=data1, locations="states", featureidkey="properties.ST_NM",
                                    color="transaction_count", color_continuous_scale="tempo",
                                    range_color=(toptryqg["transaction_count"].min(), toptryqg["transaction_count"].max()), hover_name="states",
                                    title=f"{quarter}", fitbounds="locations", height=600, width=700)
        fig_ind_2.update_geos(visible=False)
        
        st.plotly_chart(fig_ind_2)

    return toptryq
   
def toptrpins(df, states):
    ttyqu = df[df["states"]==states]
    ttyqu.reset_index(drop=True, inplace=True)

    coll3, coll4 = st.columns(2)
    with coll3:    
        fig = px.bar(ttyqu, y="transaction_amount", x="quarter",hover_data="pincodes",color="transaction_amount",
                    hover_name="pincodes", width=650, height=600)
        st.plotly_chart(fig)

    with coll4:
        fig1 = px.bar(ttyqu, y="transaction_count", x="quarter",hover_data="pincodes",color="transaction_count", 
                    hover_name="pincodes", width=650, height=600)
        st.plotly_chart(fig1)
    return ttyqu


def tpuser(df, year):
    tpus = df[df["years"]==year]
    tpus.reset_index(drop=True, inplace=True)

    tpusg = tpus.groupby(["states", "quarter"])["registered_users"].sum().reset_index()
    tpusg.reset_index(inplace=True)
    
    fig = px.bar(tpus, y="registered_users", x="states",hover_name="states",color="quarter",title=f"{year}-registered_users ",width=900, height=650)
    st.plotly_chart(fig)
    return tpus

def topuspins(df, states):
    ttyqu = df[df["states"]==states]
    ttyqu.reset_index(drop=True, inplace=True)
  
    fig = px.bar(ttyqu, y="registered_users", x="quarter",hover_data="pincodes",color= "registered_users",
                hover_name="pincodes", width=650, height=600)   

    st.plotly_chart(fig)    
    return ttyqu



def ques_transaction_count(tabname):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="123456",
        database="phonepe",
        port="5432")

    cursor = mydb.cursor()
    cursor.execute(f'''SELECT states,sum(transaction_count) as transaction_count
                from {tabname} group by states order by transaction_count desc limit 10;''')

    mydb.commit()
    table1 = cursor.fetchall()
    columns = ["states","transaction_count"]
    dec= pd.DataFrame(table1, columns=columns)


    cursor = mydb.cursor()
    cursor.execute(f'''SELECT states,sum(transaction_count) as transaction_count
                   from {tabname} group by states order by transaction_count limit 10;''')
    mydb.commit()
    table2 = cursor.fetchall()
    columns = ["states","transaction_count"]
    ase= pd.DataFrame(table2, columns=columns)


    cursor = mydb.cursor()
    cursor.execute(f'''SELECT states,avg(transaction_count) as transaction_count
                from {tabname} group by states order by transaction_count;''')

    mydb.commit()
    table3 = cursor.fetchall()
    columns = ["states","transaction_count"]
    avg= pd.DataFrame(table3, columns=columns)

    st.subheader("Top 10 and Least 10 And Avarage of Transaction Count")
    coll1, coll2 = st.columns(2)
    with coll1:  
        fig = px.bar(dec, y="transaction_count", x="states",color="states",hover_name="states",width=650, height=600)
        st.plotly_chart(fig)

    with coll2: 
        fig1 = px.bar(ase, y="transaction_count", x="states",color="states",hover_name="states",width=650, height=600)
        st.plotly_chart(fig1)

    fig2 = px.bar(avg, y="transaction_count", x="states",color="states",hover_name="states",width=1250, height=750)
    st.plotly_chart(fig2)


def ques_transaction_amount(tabname):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="123456",
        database="phonepe",
        port="5432")

    cursor = mydb.cursor()
    cursor.execute(f'''SELECT states,sum(transaction_amount) as transaction_amount
                from {tabname} group by states order by transaction_amount desc limit 10;''')

    mydb.commit()
    table1 = cursor.fetchall()
    columns = ["states","transaction_amount"]
    dec= pd.DataFrame(table1, columns=columns)


    cursor = mydb.cursor()
    cursor.execute(f'''SELECT states,sum(transaction_amount) as transaction_amount
                   from {tabname} group by states order by transaction_amount limit 10;''')
    mydb.commit()
    table2 = cursor.fetchall()
    columns = ["states","transaction_amount"]
    ase= pd.DataFrame(table2, columns=columns)


    cursor = mydb.cursor()
    cursor.execute(f'''SELECT states,avg(transaction_amount) as transaction_amount
                from {tabname} group by states order by transaction_amount;''')

    mydb.commit()
    table3 = cursor.fetchall()
    columns = ["states","transaction_amount"]
    avg= pd.DataFrame(table3, columns=columns)

    st.subheader("Top 10 and Least 10 And Avarage of Transaction Amount")

    coll1, coll2 = st.columns(2)
    with coll1:  
        fig = px.bar(dec, y="transaction_amount", x="states",color="states",hover_name="states",width=650, height=600)
        st.plotly_chart(fig)
    with coll2: 
        fig1 = px.bar(ase, y="transaction_amount", x="states",color="states",hover_name="states",width=800, height=650)
        st.plotly_chart(fig1)

    fig2 = px.bar(avg, y="transaction_amount", x="states",color="states",hover_name="states",width=1250, height=750)
    st.plotly_chart(fig2)


def ques_map_reg_users(states):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="123456",
        database="phonepe",
        port="5432")

    cursor = mydb.cursor()
    cursor.execute(f'''SELECT districts ,SUM (registered_users) as registered_users
                   FROM map_user where states='{states}'
                   group by districts order by registered_users desc limit 10''')

    mydb.commit()
    table1 = cursor.fetchall()
    columns = ["districts","registered_users"]
    dec= pd.DataFrame(table1, columns=columns)

    cursor = mydb.cursor()
    cursor.execute(f'''SELECT districts ,SUM (registered_users) as registered_users
                   FROM map_user where states='{states}'
                   group by districts order by registered_users limit 10''')

    mydb.commit()

    table2 = cursor.fetchall()
    columns = ["districts","registered_users"]
    ase= pd.DataFrame(table2, columns=columns)

    cursor = mydb.cursor()
    cursor.execute(f'''SELECT districts ,AVG (registered_users) as registered_users
                   FROM map_user where states='{states}'
                   group by districts order by registered_users''')
    mydb.commit()
    table3 = cursor.fetchall()
    columns = ["districts","registered_users"]
    avg= pd.DataFrame(table3, columns=columns)

    coll1, coll2 = st.columns(2)
    with coll1:  
        fig = px.bar(dec, y="registered_users", x="districts",color="districts",hover_name="districts",width=650, height=600)
        st.plotly_chart(fig)
    with coll2:  
        fig1 = px.bar(ase, y="registered_users", x="districts",color="districts",hover_name="districts",width=650, height=600)
        st.plotly_chart(fig1)

    fig2 = px.bar(avg, y="registered_users", x="districts",color="districts",hover_name="districts",width=1250, height=750)
    st.plotly_chart(fig2)

def ques_users_toppin(states):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="123456",
        database="phonepe",
        port="5432")
    cursor = mydb.cursor()

    cursor.execute(f'''SELECT pincodes, SUM(registered_users) as registered_users
            FROM top_user WHERE states= '{states}'
            GROUP BY pincodes ORDER BY registered_users DESC LIMIT 10;''')
    mydb.commit()

    table1 = cursor.fetchall()
    columns = ["pincodes","registered_users"]
    dec= pd.DataFrame(table1, columns=columns)


    cursor = mydb.cursor()
    cursor.execute(f'''SELECT pincodes ,SUM (registered_users) as registered_users
                   FROM top_user where states='{states}'
                   group by pincodes order by registered_users limit 10''')

    mydb.commit()

    table2 = cursor.fetchall()
    columns = ["pincodes","registered_users"]
    ase= pd.DataFrame(table2, columns=columns)

    cursor = mydb.cursor()
    cursor.execute(f'''SELECT pincodes ,AVG (registered_users) as registered_users
                   FROM top_user where states='{states}'
                   group by pincodes order by registered_users''')
    mydb.commit()

    table3 = cursor.fetchall()
    columns = ["pincodes","registered_users"]
    avg= pd.DataFrame(table3, columns=columns)

    coll1, coll2 = st.columns(2)
    with coll1:    
        fig = px.pie(dec, values="registered_users", names="pincodes", color="pincodes", hover_name="pincodes",width=500, height=550)
        st.plotly_chart(fig)
    with coll2:   
        fig1 = px.pie(dec, values="registered_users", names="pincodes", color="pincodes", hover_name="pincodes", width=500, height=550)
        st.plotly_chart(fig1)

    fig2 = px.pie(dec, values="registered_users", names="pincodes", color="pincodes", hover_name="pincodes", width=650, height=600)
    st.plotly_chart(fig2)

def ques_map_appopen_users(states):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="123456",
        database="phonepe",
        port="5432")

    cursor = mydb.cursor()
    cursor.execute(f'''SELECT districts ,SUM (app_opens) as app_opens
                   FROM map_user where states='{states}'
                   group by districts order by app_opens desc limit 10''')

    mydb.commit()
    table1 = cursor.fetchall()
    columns = ["districts","app_opens"]
    dec= pd.DataFrame(table1, columns=columns)

    cursor = mydb.cursor()
    cursor.execute(f'''SELECT districts ,SUM (app_opens) as app_opens
                   FROM map_user where states='{states}'
                   group by districts order by app_opens limit 10''')

    mydb.commit()

    table2 = cursor.fetchall()
    columns = ["districts","app_opens"]
    ase= pd.DataFrame(table2, columns=columns)

    cursor = mydb.cursor()
    cursor.execute(f'''SELECT districts ,AVG (app_opens) as app_opens
                   FROM map_user where states='{states}'
                   group by districts order by app_opens''')
    mydb.commit()
    table3 = cursor.fetchall()
    columns = ["districts","app_opens"]
    avg= pd.DataFrame(table3, columns=columns)

    coll1, coll2 = st.columns(2)
    with coll1:
        fig = px.bar(dec, y="app_opens", x="districts",color="districts",hover_name="districts",width=600, height=650)
        st.plotly_chart(fig)
    with coll2:
        fig1 = px.bar(ase, y="app_opens", x="districts",color="districts",hover_name="districts",width=600, height=650)
        st.plotly_chart(fig1)

    fig2 = px.bar(avg, y="app_opens", x="districts",color="districts",hover_name="districts",width=1250, height=750)
    st.plotly_chart(fig2)
    
import streamlit as st

st.set_page_config(layout="wide")

with st.sidebar:
    
    selected = option_menu("Main Menu", ["Intro",'Top Chart','Explore Data'], 
        icons=['house','search','gear','phone'])

if selected=="Intro":
    st.title("*:violet[Welcome to PhonePe]")

    st.markdown(""":violet[**PhonePe is a payments app that allows you to use BHIM UPI,
                your credit card and debit card or wallet to recharge your mobile phone,
                pay all your utility bills and to make instant payments at your favourite offline and online stores.**]""")
    
    st.markdown(""":violet[**You can also invest in mutual funds and buy insurance plans on PhonePe. Get Car & Bike Insurance on our app.
                Link your bank actransaction_count on PhonePe and transfer money with BHIM UPI instantly! 
                The PhonePe app is safe and secure, meets all your payment, investment, mutual funds,
                insurance and banking needs, and is much**]""")
    
    import streamlit as st

elif selected == "Top Chart":
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="123456",
        database="phonepe",
        port="5432")

    cursor = mydb.cursor()

    question= st.selectbox("Select the Question",[ 
                 
                "1. Total transaction_amount and transaction_count of Aggregated Transaction",

                "2. Total transaction_amount and transaction_count of Map Transaction",

                "3. Total transaction_amount and transaction_count of Top Transaction",

                "4. Total transaction_count of Aggregated User",

                "5. Registered users of Map User",

                "6. App opens of Map User",

                "7. Registered users of Top User"])
    
    if question =="1. Total transaction_amount and transaction_count of Aggregated Transaction":
        ques_transaction_amount("agg_tran")
        ques_transaction_count("agg_tran")

    if question =="2. Total transaction_amount and transaction_count of Map Transaction":
        ques_transaction_amount("map_tran")
        ques_transaction_count("map_tran")
    
    if question =="3. Total transaction_amount and transaction_count of Top Transaction":
        ques_transaction_amount("top_tran")
        ques_transaction_count("top_tran")

    if question =="4. Total transaction_count of Aggregated User":
        ques_transaction_count("agg_user")
        

    if question =="5. Registered users of Map User":
        states=st.selectbox('Select a states',map_user["states"].unique())
        ques_map_reg_users(states)

    if question =="6. App opens of Map User":
        states=st.selectbox('Select a states',top_user["states"].unique())
        ques_map_appopen_users(states)

    if question =="7. Registered users of Top User":
        st.subheader('Registered users of Top User', divider='rainbow')
        states=st.selectbox('Select a states',top_user["states"].unique())
        ques_users_toppin(states)
             

elif selected == "Explore Data":

    tab1, tab2, tab3 = st.tabs(["***Aggregated Analysis***", "***Map Analysis***", "***Top Analysis***"])  

    with tab1:
        anal = ["agg_tran", "agg_user"]
        tab_selected = st.radio("Select Tab", anal)

        if tab_selected == "agg_tran":

            year = st.slider('Select a year', min_value=2018, max_value=2023, step=1, key='unique_slider_key_1')
            tacy=tran_transaction_amount_year(agg_tran,year)

            quarter = st.select_slider('Select a quarter',options=[1,2,3,4])         
            tacyq=tran_transaction_amount_year_quarter(tacy,quarter) 

            states=st.selectbox('Select a states',tacy["states"].unique())
            transaction_type(tacyq,states)


        elif tab_selected == "agg_user":             
            year = st.slider('Select a year', min_value=2018, max_value=2022, step=1, key='unique_slider_key_1') 
            quart = st.select_slider('Select a quarter', options=[1, 2, 3, 4])      

            brandsf=brands(agg_user, year)
            stus=brandsqu(brandsf, quart)

            states=st.selectbox('Select a states',agg_user["states"].unique())
            brandstates(stus, states, year)
            

    with tab2:
        anal2 = ["map_tran", "Map_user"]
        tab_selected = st.radio("Select Tab", anal2)       

        if tab_selected == "map_tran":
            states = st.selectbox('Select a states', agg_user["states"].unique(), key='states_selectbox')
            mapdisttr(map_tran,states)
            
        elif tab_selected == "Map_user":
            states = st.selectbox('Select a states', agg_user["states"].unique(), key='states_selectbox')
            mapdistus(map_user, states)

    with tab3:
        anal3 = ["top_tran", "Top_user"]
        tab_selected = st.radio("Select Tab", anal3)        

        if tab_selected == "top_tran":
            year = st.slider('Select a year', min_value=2018, max_value=2023, step=1, key='unique_slider_key_3')
            tty=top_tran_transaction_amount_year(top_tran, year)

            quarter = st.select_slider('Select a quarter',options=[1,2,3,4],key='unique_slider_key_9')
            ttyq=top_tran_transaction_amount_year_quarter(tty,quarter)

            states = st.selectbox('Select a states', top_tran["states"].unique(), key='states_selectbox_9')
            toptrpins(tty,states)
            
        elif tab_selected == "Top_user":
            year = st.slider('Select a year', min_value=2018, max_value=2023, step=1, key='unique_slider_key_8')
            tpusy=tpuser(top_user, year)

            states = st.selectbox('Select a states', top_tran["states"].unique(), key='states_selectbox_9')
            topuspins(tpusy, states)


