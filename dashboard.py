import streamlit as st
import pandas as pd
from data_process import process_sheets

def main():
    st.title("Excel Data Processing Dashboard")
    st.write("Upload an Excel file to process and clean the data.")

    uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx"])
    if uploaded_file:
        st.write("Processing the uploaded file...")
        output_data = process_sheets(uploaded_file)

        st.write("Processed Data:")
        st.dataframe(output_data)

        output_excel = "processed_data.xlsx"
        output_data.to_excel(output_excel, index=False)
        with open(output_excel, "rb") as f:
            st.download_button(
                label="Download Processed Excel File",
                data=f,
                file_name="processed_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

if __name__ == "__main__":
    main()
