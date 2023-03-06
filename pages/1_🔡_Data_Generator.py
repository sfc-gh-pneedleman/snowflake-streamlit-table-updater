
##import python libraries  
import snowflake.connector
import streamlit as st
import string


st.set_page_config(page_title="Data Generator", page_icon="🔡", layout="wide") 


st.header('Let\'s create a sample table with a primary key 🔡')

def get_data():

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

    #open a snowflake cursor
    cs = ctx.cursor()

    #formatting to contain the text box to only one column, so it doesnt span the entire width
    col1, col2 = st.columns(2)

    #formatting to contain the text box to only one column, so it doesnt span the entire width
    with col1:
        st.subheader("What would you like to name your table?")
        table_nanme = st.text_input('Table Name')

        if table_nanme:
            st.subheader("You entered: ", table_nanme)
    
            create_tbl_stmt =   "CREATE OR REPLACE TABLE  " +  table_nanme + \
            " AS                                  \
            select                              \
            SEQ4()+1 ID,                        \
            case uniform(1, 6, random() )       \
                when 1 then 'Eggs'              \
                when 2 then 'Milk'              \
                when 3 then 'Bread'             \
                when 4 then 'Butter'            \
                when 5 then 'Sugar'             \
                when 6 then 'Flour'             \
                end as product,                 \
            ROUND(uniform(1::float, 12::float, random()), 2) SALE_PRICE,        \
            last_day(dateadd('month', row_number() over (order by 1), '2021-12-01')::date) SALE_date   \
            FROM TABLE(GENERATOR(rowcount => 20))"

            cs.execute(create_tbl_stmt)

            alter_pk_stmt = "ALTER TABLE "+  table_nanme + "  ADD PRIMARY KEY (ID)"
            cs.execute(alter_pk_stmt)


            st.success("Table  " + table_nanme +  " successfully created in " + string.sf_database + "." + string.sf_schema + 
                        ". Please navigate to [Table Updater](http://localhost:8501/Table_Updater) to edit your table" )

            select_stmt = "SELECT * FROM  " +  table_nanme    

            cs.execute(select_stmt)
            df = cs.fetch_pandas_all()
            st.dataframe(df, use_container_width=True)


if __name__ == "__main__":

    df = get_data()
