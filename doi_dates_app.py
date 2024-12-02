import streamlit as st
import pandas as pd
import requests
import json
import time
from datetime import datetime
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
import glob
warnings.filterwarnings('ignore')

class DOIProcessor:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Research_Paper_Analyzer/1.0 (mailto:your-email@example.com)'
        }
        self.results = []
        self.errors = []
        
    def combine_csv_files(self, uploaded_files):
        """Combine all uploaded CSV files"""
        if not uploaded_files:
            st.error("No files uploaded")
            return None
            
        st.write(f"Processing {len(uploaded_files)} files:")
        
        dfs = []
        for uploaded_file in uploaded_files:
            try:
                df = pd.read_csv(uploaded_file)
                dfs.append(df)
                st.write(f"✓ Loaded {uploaded_file.name}: {len(df)} rows")
            except Exception as e:
                st.error(f"Error loading {uploaded_file.name}: {str(e)}")
                
        if not dfs:
            return None
            
        combined_df = pd.concat(dfs, ignore_index=True)
        st.write(f"Total combined rows: {len(combined_df)}")
        return combined_df
        
    def get_paper_date(self, doi):
        """Get created date for a single DOI"""
        if pd.isna(doi):
            return doi, "Not available"
            
        url = f'https://api.crossref.org/works/{doi}'
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if 'message' in data and 'created' in data['message']:
                    date_parts = data['message']['created']['date-parts'][0]
                    if len(date_parts) >= 2:
                        return str(doi), f"{date_parts[0]}-{date_parts[1]:02d}"
                    return str(doi), str(date_parts[0])
            time.sleep(0.1)
            return str(doi), "Not available"
        except Exception as e:
            self.errors.append(f"Error with DOI {doi}: {str(e)}")
            return str(doi), "Error"

    def filter_by_date_range(self, df, start_date=None, end_date=None):
        """Filter DataFrame by date range"""
        if start_date is None and end_date is None:
            return df
            
        try:
            # Convert Created Date to datetime
            df['Created Date'] = pd.to_datetime(df['Created Date'], format='%Y-%m', errors='coerce')
            
            # Apply date filters
            if start_date:
                start_date = pd.to_datetime(start_date)
                df = df[df['Created Date'] >= start_date]
            
            if end_date:
                end_date = pd.to_datetime(end_date)
                df = df[df['Created Date'] <= end_date]
            
            # Convert back to original format
            df['Created Date'] = df['Created Date'].dt.strftime('%Y-%m')
            
            return df
            
        except Exception as e:
            st.error(f"Error filtering dates: {str(e)}")
            return df

    def process_dois(self, df, progress_bar, max_workers=4):
        """Process all DOIs with parallel execution"""
        try:
            # Find the DOI column
            doi_column = None
            for col in df.columns:
                if 'DOI' in str(col).upper():
                    doi_column = col
                    break
            
            if doi_column is None:
                st.error("Error: Could not find DOI column")
                return None, None
                
            # Get DOIs from the identified column
            dois = df[doi_column].astype(str).tolist()
            dois = [doi for doi in dois if pd.notna(doi) and doi != 'nan']
            st.write(f"Found {len(dois)} unique DOIs to process")
            
            if not dois:
                st.error("No DOIs found in the files")
                return None, None
                
            # Process DOIs in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for doi in dois:
                    future = executor.submit(self.get_paper_date, doi)
                    futures.append(future)
                
                # Initialize counter for progress
                completed = 0
                total = len(futures)
                
                # Update progress bar
                for future in as_completed(futures):
                    result = future.result()
                    if result[1] != "Not available":
                        self.results.append(result)
                    completed += 1
                    progress_bar.progress(min(1.0, completed / total))
            
            # Convert results to dictionary
            dates_dict = {str(k): v for k, v in dict(self.results).items()}
            
            # Add results to DataFrame as the last column
            df['Created Date'] = df[doi_column].astype(str).map(dates_dict)
            
            # Reorder columns to move 'Created Date' to the end
            cols = df.columns.tolist()
            if 'Created Date' in cols:
                cols.remove('Created Date')
                cols.append('Created Date')
                df = df[cols]
            
            # Convert to datetime for sorting
            df['Created Date'] = pd.to_datetime(df['Created Date'], format='%Y-%m', errors='coerce')
            
            # Sort by Created Date
            df = df.sort_values('Created Date', ascending=True)
            
            # Convert back to string format
            df['Created Date'] = df['Created Date'].dt.strftime('%Y-%m')
            
            # Set progress to complete
            progress_bar.progress(1.0)
            
            return df, dates_dict
            
        except Exception as e:
            st.error(f"Error processing DOIs: {str(e)}")
            return None, None

def main():
    st.title("DOI Date Retriever")
    st.write("Upload CSV files containing DOIs to retrieve their creation dates")
    
    # Initialize session state
    if 'processed_data' not in st.session_state:
        st.session_state.processed_data = None
    if 'filtered_data' not in st.session_state:
        st.session_state.filtered_data = None
    if 'dates_dict' not in st.session_state:
        st.session_state.dates_dict = None
    
    # File uploader
    uploaded_files = st.file_uploader("Choose CSV files", type="csv", accept_multiple_files=True)
    
    # Date range inputs
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date (optional)", value=None)
    with col2:
        end_date = st.date_input("End Date (optional)", value=None)
    
    if uploaded_files:
        if st.button("Process DOIs") or st.session_state.processed_data is not None:
            if st.session_state.processed_data is None:  # Only process if not already done
                processor = DOIProcessor()
                
                # Combine files
                df = processor.combine_csv_files(uploaded_files)
                
                if df is not None:
                    # Create progress bar
                    progress_bar = st.progress(0)
                    st.write("Processing DOIs...")
                    
                    # Process DOIs
                    processed_df, dates_dict = processor.process_dois(df, progress_bar)
                    
                    if processed_df is not None:
                        st.session_state.processed_data = processed_df
                        st.session_state.dates_dict = dates_dict
            
            # Use processed data from session state
            if st.session_state.processed_data is not None:
                df = st.session_state.processed_data
                dates_dict = st.session_state.dates_dict
                
                # Filter by date range if specified
                if start_date or end_date:
                    processor = DOIProcessor()
                    filtered_df = processor.filter_by_date_range(
                        df.copy(), start_date, end_date)
                    
                    if len(filtered_df) > 0:
                        st.session_state.filtered_data = filtered_df
                        st.write(f"Found {len(filtered_df)} records in date range")
                        st.write("Filtered Results:")
                        st.dataframe(filtered_df)
                        
                        # Download button for filtered results
                        csv_filtered = filtered_df.to_csv(index=False)
                        st.download_button(
                            label="Download Filtered Results",
                            data=csv_filtered,
                            file_name=f"doi_dates_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                    else:
                        st.warning("No records found in the specified date range")
                
                # Show full results
                st.write("Full Results:")
                st.dataframe(df)
                
                # Download button for full results
                csv_full = df.to_csv(index=False)
                st.download_button(
                    label="Download Full Results",
                    data=csv_full,
                    file_name=f"doi_dates_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
                # Show summary
                st.write("Summary:")
                total_found = sum(1 for date in dates_dict.values() 
                                if date not in ["Not available", "Error"])
                st.write(f"Total DOIs processed: {len(dates_dict)}")
                st.write(f"DOIs with dates found: {total_found}")
                st.write(f"DOIs without dates: {len(dates_dict) - total_found}")
    
    # Clear results button
    if st.session_state.processed_data is not None:
        if st.button("Clear Results"):
            st.session_state.processed_data = None
            st.session_state.filtered_data = None
            st.session_state.dates_dict = None
            st.experimental_rerun()

if __name__ == "__main__":
    main()
