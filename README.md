# snowflake-streamlit-table-updater
CRUD on a Snowflake table via Streamlit

As of v1.19, Streamlit enables a Dataframe editor. This is an awesome feature but only natively works with Python. This streamlit app add integration with Snowflake , and could be modified for other DBs as well.

More info on the streamlit Dataframe editor here: 
 - Blog: https://blog.streamlit.io/editable-dataframes-are-here/ 
 - Documentation: https://docs.streamlit.io/library/advanced-features/dataframes
 
 
 ![](https://blog.streamlit.io/content/images/2023/02/data-editor-add-delete-10.44.28-AM-1.gif)

### Current Limitations:
 - Only tables with 1 Primary Key defined are supported 
 - Primary keys can be edited which triggers a new row to be inserted (no current way to disable editing PKs)
 - Dates and Timestamps are not supported by the dataframe editor 
 - no way to sort the dataframe if using "dynamic mode" (inserts). You can search though. 
