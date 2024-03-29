# snowflake-streamlit-table-updater
CRUD on a Snowflake table via Streamlit

Update: March 16, 2024:
Streamlit in Snowflake (SiS) has been upgraded to v1.26 in Public Preview. Due to this, the experimental Dataframe editor is no longer experiemental. There are two versions of the SiS code withih the directory; one for pre v1.26 and another for 1.26+. Hybrid tables (with PK) are also supported. 
Release notes: https://docs.snowflake.com/en/release-notes/2024/other/2024-03-15#streamlit-in-snowflake-support-for-streamlit-1-26-0-preview


As of v1.19, Streamlit enables a Dataframe editor. This is an awesome feature but only natively works with Python. This streamlit app add integration with Snowflake , and could be modified for other DBs as well. 
The code for the experimental data frame was signigiantly changed as part of v1.23. This codebase *only supports 1.23+* 

More info on the streamlit Dataframe editor here: 
 - Blog: https://blog.streamlit.io/editable-dataframes-are-here/ 
 - Documentation: https://docs.streamlit.io/library/advanced-features/dataframes
 
 
 ![](https://blog.streamlit.io/content/images/2023/02/data-editor-add-delete-10.44.28-AM-1.gif)

### Current Limitations:
 - Only tables with 1 Primary Key defined are supported
 - Primary keys are disabled from being edited. To support this, a unique ID must be defined for the PK outside streamlit, such as an IDENTIY column 
 - no way to sort the dataframe if using "dynamic mode" (inserts). You can search though. 
