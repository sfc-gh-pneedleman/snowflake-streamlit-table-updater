import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector
import string
import json

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
table_name = st.text_input('Table Name')
if table_name:
    st.write("You entered: ", table_name)
    
    select_stmt = "SELECT * FROM  " +  table_name    

    cs.execute(select_stmt)
    df = cs.fetch_pandas_all()

    #df.set_index("ID", inplace=True)

    edited_df = st.experimental_data_editor(df, key="data_editor", use_container_width=True, num_rows="dynamic") # 👈 Set a key
    st.write("Here's the session state:")
    st.write(st.session_state["data_editor"])
    
    json_raw = st.session_state["data_editor"]
    
   

    submit =st.button("Submit Changes")
    if submit: 
        
         
        
        for key in json_raw:
            value = json_raw[key]
            #st.write("The key and value are ({}) = ({})".format(key, value))
            if key == "deleted_rows":
                temp_df = pd.DataFrame.from_dict(json_raw['deleted_rows'])
                temp_df.columns = ['VAL']
                #st.write(df_new)
                delete_df = pd.merge(temp_df, df, left_on='VAL', right_index=True)
                #st.write(delete_df)

                col_list = str(tuple(delete_df['ID'].astype(str).tolist()))
                delete_stmt = "DELETE FROM " + table_name + " WHERE ID IN " + col_list
                cs.execute(delete_stmt)
                st.success('Data Deleted - Rows: ' + col_list)

                



                

                    
                    
                


