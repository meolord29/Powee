import streamlit as st
import pandas as pd
import plotly.express as px

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Energy Dashboard Skeleton",
    layout="wide"
)

# --- PLACEHOLDER FUNCTIONS ---
# You will replace the logic in these functions with your own.

def get_data_from_database(provider_table, time_range):
    """
    Placeholder for your data fetching logic.
    Connects to your database and retrieves data based on user filters.
    """
    st.info(f"Querying table '{provider_table}' for the '{time_range}'...")
    # TODO: Implement your database connection and query logic here.
    # Example:
    # with get_db_connection() as conn:
    #     query = f"SELECT * FROM {provider_table} WHERE ..."
    #     df = pd.read_sql(query, conn)
    # For now, we return an empty DataFrame to avoid errors.
    return pd.DataFrame()

def calculate_total_bill(df):
    """Placeholder for calculating the total electric bill."""
    # TODO: Implement your bill calculation logic here based on the dataframe.
    # Example:
    # if not df.empty:
    #     return f"${df['cost'].sum():,.2f}"
    return "$[Total Bill]"

def calculate_server_bill(df):
    """Placeholder for calculating the server-specific electric bill."""
    # TODO: Implement your server-specific bill calculation.
    # Example:
    # if not df.empty:
    #     server_cost = df[df['component'] == 'Servers']['cost'].sum()
    #     return f"${server_cost:,.2f}"
    return "$[Server Bill]"

def create_consumption_graph(df):
    """Placeholder for creating the Plotly graph."""
    # TODO: Implement your graph creation logic.
    # The graph should visualize data from the filtered dataframe.
    # Example:
    # if not df.empty:
    #     fig = px.line(df, x='timestamp', y='consumption_kwh', color='component')
    #     return fig
    # Return None if no data, so we can handle it gracefully.
    return None

def run_sql_query(query_string):
    """Placeholder for running a user-defined SQL query."""
    st.info(f"Executing query...")
    # TODO: Connect to your database and run the raw SQL query.
    # Handle potential SQL errors gracefully with a try-except block.
    # Example:
    # with get_db_connection() as conn:
    #     result_df = pd.read_sql(query_string, conn)
    # For now, we return a sample DataFrame.
    sample_data = {
        'column_a': [1, 2, 3],
        'column_b': ['X', 'Y', 'Z'],
        'message': ['This is sample data.', 'Replace with your query result.', '']
    }
    return pd.DataFrame(sample_data)

# --- PAGE 1: Home ---
def render_home_page():
    """Renders the main dashboard page."""
    st.title("Energy Consumption Dashboard")

    # --- LAYOUT: TOP ROW FOR FILTERS ---
    col1, col2 = st.columns([1, 1])

    # --- Top Left Corner ---
    with col1:
        st.subheader("Time Range Selection")
        time_range = st.selectbox(
            "Select the time range for the analysis:",
            ("Last Month", "Last 2 Months", "Last Quarter"),
            label_visibility="collapsed"
        )

    # --- Top Right Corner ---
    with col2:
        st.subheader("Data Source")
        
        # Behind the scenes, these names should map to your table names
        provider = st.selectbox(
            "Select Electricity Provider:",
            ("Provider A (e.g., Pacific Power)", "Provider B (e.g., Evergreen Electric)"),
            label_visibility="collapsed"
        )
        
        uploaded_file = st.file_uploader(
            "Or upload a CSV file",
            type=['csv']
        )
        if uploaded_file is not None:
            # TODO: Add logic to process the uploaded CSV file.
            st.success("File uploaded! (Processing logic to be added)")

    # --- METRICS DISPLAY (under the left-side filters) ---
    st.markdown("---")
    metric_col1, metric_col2 = st.columns(2)

    with metric_col1:
        st.metric(
            label="Total Electric Bill",
            value=calculate_total_bill(None) # Pass the dataframe here
        )

    with metric_col2:
        st.metric(
            label="Total Server Electric Bill",
            value=calculate_server_bill(None) # Pass the dataframe here
        )
    st.markdown("---")

    # --- MAIN CHART SECTION ---
    st.subheader("Electricity Consumption Analysis")
    
    # Placeholder for the graph
    graph_placeholder = st.empty()
    
    # This is where you would call your functions
    # 1. Get filtered data
    # filtered_df = get_data_from_database(provider, time_range)
    # 2. Create graph
    # fig = create_consumption_graph(filtered_df)
    
    # if fig:
    #     graph_placeholder.plotly_chart(fig, use_container_width=True)
    # else:
    #     graph_placeholder.info("Graph will be displayed here once data is loaded and processed.")
    
    graph_placeholder.info("Graph will be displayed here. Update `create_consumption_graph` to show your plot.")


# --- PAGE 2: Query ---
def render_query_page():
    """Renders the page for running custom SQL queries."""
    st.title("Database Query Tool")

    query_string = st.text_area(
        "Enter your SQL query",
        height=250,
        placeholder="SELECT * FROM your_table_name LIMIT 10;"
    )

    if st.button("Run Query"):
        if query_string:
            # --- Call your function to execute the query ---
            result_df = run_sql_query(query_string)
            
            st.subheader("Query Result")
            st.dataframe(result_df, use_container_width=True)
            
            # --- Allow user to download results ---
            st.download_button(
                label="Download data as CSV",
                data=result_df.to_csv(index=False).encode('utf-8'),
                file_name='query_result.csv',
                mime='text/csv',
            )
        else:
            st.warning("Please enter a query to run.")

# --- SIDEBAR AND NAVIGATION ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Choose a page", ["Home", "Query"])

if page == "Home":
    render_home_page()
elif page == "Query":
    render_query_page()