import streamlit as st
import string
import snowflake.connector

st.set_page_config(
    page_title="Snowflake Table Updater"
)

st.title("Snowflake Table Updated ❄️")

st.write("This is a sample app to demo the ability to use Snowflake and Streamlit for CRUD")   
st.write("Please authenticate to the left to get started")   
st.write("")

st.warning("As of Streamlit v1.23 the experimental data editor changed to data editor. The output of this object has signifiganrly changed from previous version. This codebase has been updated to support 1.23+")
st.write("Please use version 1.23 of streamlit to use this new experimental feature")
st.write("docs: https://docs.streamlit.io/library/api-reference/widgets/st.experimental_data_editor and https://docs.streamlit.io/library/advanced-features/dataframes#access-edited-data")



st.sidebar.info("Please enter your Snowflake connection info, and then select an application above.")


sf_user_in = st.sidebar.text_input("username")
sf_password_in = st.sidebar.text_input("Password", type='password')
sf_account_in = st.sidebar.text_input("Account Name", help="This can be in the form of <org-account> or <account_locator.region>. More info: https://docs.snowflake.com/en/user-guide/admin-account-identifier.html")
sf_role_in = st.sidebar.text_input("Role",  value="SYSADMIN")
sf_wh_in = st.sidebar.text_input("Warehouse",  value="COMPUTE_WH")
sf_db_in = st.sidebar.text_input("Database", value="SNOW_DB")
sf_schema_in = st.sidebar.text_input("Schema",  value="SNOW_SCHEMA")
# Every form must have a submit button.
submitted = st.sidebar.button("Conneect")
if submitted:
    #########################################################
    ###### SET SNOWFLAKE CONN PARAMS ONCE  ##################
    #Its not good practice to include passwords in your code. 
    # This is here for demo purposes
    string.sf_user = sf_user_in
    string.sf_password= sf_password_in
    string.sf_account=sf_account_in
    string.sf_role = sf_role_in
    string.sf_warehouse=sf_wh_in
    string.sf_database=sf_db_in
    string.sf_schema=sf_schema_in

    #########################################################
    try:
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

        st.sidebar.success("Successfully Connected as: " + sf_user_in +  '\n' + '\n' +   \
            "You can proceed to the other pages. This connection information will be stored for your session.")
    except Exception as e:
        st.sidebar.error("Connection Failed. Please try again! The pages will not work unless a succsfull connection is made" + '\n' + '\n' + "error: " + str(e))
 
    
    st.success("Successfully Connected as: " + sf_user_in +  '\n' + '\n' )
    
    st.write("do you need data? if so click data generator, otherwise goto table updater ")
   