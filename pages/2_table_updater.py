from jsonschema import Draft202012Validator
import streamlit as st
import pandas as pd
import pydeck as pdk
from urllib.error import URLError
import numpy as np
import snowflake.connector
import string

st.set_page_config(page_title="Table Editor", page_icon="🌍", layout="wide")

##add some markdown to the page with a desc 
st.header("Lets get Editing")

##snowflake connection info. Its not good practice to include passwords in your code. This is here for demo purposes
ctx = snowflake.connector.connect(
    user=string.sf_user,
    password=string.sf_password, 
    account=string.sf_account,
    warehouse=string.sf_warehouse,
    database=string.sf_database,
    schema=string.sf_schema,
    role=string.sf_role
)

#open the connection
cs = ctx.cursor()

st.write("What would you like to name your table")
table_nanme = st.text_input('Table Name')
if table_nanme:
    st.write("You entered: ", table_nanme)
    
    select_stmt = "SELECT * FROM  " +  table_nanme    

    cs.execute(select_stmt)
    df = cs.fetch_pandas_all()

    df.set_index("ID", inplace=True)

    edited_df = st.experimental_data_editor(df, key="data_editor", use_container_width=True, num_rows="dynamic") # 👈 Set a key
    st.write("Here's the session state:")
    st.write(st.session_state["data_editor"]) #

    df.columns[1]

    submit =st.button("Submit Changes")
    #if submit: 
    #
    #    sql= "CREATE OR REPLACE TABLE TMP (JSON_DATA VARIANT)"
    #    cs.execute(sql)
    #    sql_insert ="INSERT INTO TMP  SELECT PARSE_JSON(TO_JSON("+ str(st.session_state["data_editor"]) + "))"
    #    st.write(sql_insert)
    #    cs.execute(sql_insert)

