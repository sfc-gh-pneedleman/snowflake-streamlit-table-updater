import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector
import string
import json

st.set_page_config(page_title="Table Editor", page_icon="📋", layout="wide")

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

table_list_sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
                    WHERE TABLE_SCHEMA = '" + string.sf_schema + "' AND TABLE_TYPE = 'BASE TABLE'"
cs.execute(table_list_sql)
table_list_df = cs.fetch_pandas_all()


st.write("Select the table you'd like to edit")
table_name = st.selectbox('Table Name',table_list_df)
if table_name:
    st.write("You entered: " +  table_name)
    
    get_PK_sql = "show primary keys in " + table_name
    #st.write(get_PK_sql)
    cs.execute(get_PK_sql)
    cs.get_results_from_sfqid(cs.sfqid)
    results = cs.fetchall()
    if len(results) != 1 :
        st.error('Only tables with 1 PK column are supported: Your Table has less than 1 or more than 1 PK column')
    
    for rec in results :
        PK_COL = (rec[4])

        select_stmt = "SELECT * FROM  " +  table_name    

        cs.execute(select_stmt)
        df = cs.fetch_pandas_all()


        #df.set_index("ID", inplace=True)

        edited_df = st.experimental_data_editor(df, key="data_editor", use_container_width=True, num_rows="dynamic") # 👈 Set a key
        st.write("Here's the session state:")
        st.write(st.session_state["data_editor"])
        
        json_raw = st.session_state["data_editor"]
    
        submit =st.button("Submit Changes")
    
    #df_pk = cs.fetch_pandas_all()
    #st.dataframe(df_pk)
   

  
        if submit: 

            def remove_comma_from_list(input_list):
                if int(len(input_list)) == 1:
                    output_list = str(input_list).replace(',', '')
                else:
                    output_list = str(input_list)
                        
                return output_list    
            
            for key in json_raw:
                value = json_raw[key]
                #st.write("The key and value are ({}) = ({})".format(key, value))

                #handle edit 
                # if key == "edited_cells" and  len(json_raw['edited_cells']) > 0:
                #     temp_df_1 = pd.DataFrame.from_dict(json_raw['edited_cells'], orient='index', columns=['VAL'])
                #      #temp_df_1 = temp_df_1.reset_index()
                #      #temp_df_1['index_col'] = temp_df_1.index
                #     temp_df_1.reset_index(inplace=True)
                #     temp_df_1= temp_df_1.rename(columns={"index": "KEY"})
                #     temp_df_1[['COL','ROW']] = temp_df_1.KEY.str.split(":",expand=True)
                #      #temp_df_2 = pd.DataFrame.from_dict(json_raw['added_rows'])
                #     st.write(temp_df_1)


                if key == "added_rows" and len(json_raw['added_rows']) > 0 :
                    st.write('insert statements:')
                    for key in json_raw['added_rows']:
                        #st.write(key)
                        temp_df_2 = pd.DataFrame.from_dict(key, orient='index', columns=['VAL'])
                        temp_df_2= temp_df_2.T
                     #temp_df_1 = temp_df_1.reset_index()
                     #temp_df_1['index_col'] = temp_df_1.index
                     #temp_df_2 = pd.DataFrame.from_dict(json_raw['added_rows'])
                        #st.write(temp_df_2)
                        #rename columns so we get the column names from the orig DF based on the values  that chaged from those columns
                        for col in temp_df_2.columns:
                            temp_df_2.rename(columns={str(col): df.columns[int(col)-1]}, inplace=True)
                        col_list = tuple(temp_df_2.columns.values.tolist())
                        row_values = tuple(temp_df_2.iloc[0].tolist())
                        
                        col_list= remove_comma_from_list(col_list)
                        row_values = remove_comma_from_list(row_values)

                        insert_statement ="INSERT INTO " + table_name + " " + col_list + " VALUES " + row_values
                        st.write(insert_statement)

                # # #handle delete logic and check if there are values in the deleted rows key
                if key == "deleted_rows" and len(json_raw['deleted_rows']) > 0:
                    st.write('delete statements:')
                    temp_df = pd.DataFrame.from_dict(json_raw['deleted_rows'])
                    temp_df.columns = ['VAL']
                    #st.write(df_new)
                    delete_df = pd.merge(temp_df, df, left_on='VAL', right_index=True)
                    #st.write(delete_df)
                    #convert column from pandas to comma separated list
                    col_list = tuple(delete_df['ID'].astype(str).tolist())
                    #if length = 1we need to remove last , 
                    col_list= remove_comma_from_list(col_list)
 
                    delete_stmt = "DELETE FROM " + table_name + " WHERE " + PK_COL + " IN " + col_list
                    st.write(delete_stmt)
                    #cs.execute(delete_stmt)
                    #st.success('Data Deleted - Rows: ' + col_list)

                    



                    

                        
                        
                    


