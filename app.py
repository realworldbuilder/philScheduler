import streamlit as st
import pandas as pd
import tempfile
import os


class XerParser:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def parse_task_table(self):
        """Parse TASK table with handling for missing fields"""
        task_data = []
        in_task_table = False
        task_fields = None
        row_count = 0

        key_fields = [
            'task_id', 'proj_id', 'wbs_id', 'task_code', 'task_name',
            'phys_complete_pct', 'status_code', 'target_drtn_hr_cnt',
            'act_start_date', 'act_end_date', 'target_start_date',
            'target_end_date'
        ]

        with open(self.file_path, 'r', encoding='windows-1252') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                if line.startswith('%T'):
                    current_table = line[3:].strip()
                    if current_table == 'TASK':
                        in_task_table = True
                    else:
                        in_task_table = False

                elif in_task_table:
                    if line.startswith('%F'):
                        task_fields = line[3:].split('\t')
                        st.write(f"Found {len(task_fields)} fields")

                    elif line.startswith('%R'):
                        row_count += 1
                        values = line[3:].split('\t')

                        # Add empty value for missing field
                        if len(values) < len(task_fields):
                            values.append('')

                        # Create row data with all fields
                        row_data = dict(zip(task_fields, values))

                        # Clean up the data
                        for field in ['phys_complete_pct', 'target_drtn_hr_cnt']:
                            if field in row_data:
                                try:
                                    row_data[field] = float(row_data[field])
                                except (ValueError, TypeError):
                                    row_data[field] = 0.0

                        task_data.append(row_data)

        if task_data:
            df = pd.DataFrame(task_data)

            # Select and reorder important columns
            available_fields = [f for f in key_fields if f in df.columns]
            return df[available_fields]

        return None


def main():
    st.title("XER Task Data Extractor")
    st.write("Upload your P6 XER file to extract task data")

    uploaded_file = st.file_uploader("Choose an XER file", type=['xer'])

    if uploaded_file is not None:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xer', mode='wb') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            temp_path = tmp_file.name

        try:
            parser = XerParser(temp_path)
            df = parser.parse_task_table()

            if df is not None and not df.empty:
                st.write("### Task Data Summary")
                st.write(f"Number of tasks: {len(df)}")

                # Display completion statistics
                completed = len(df[df['phys_complete_pct'] == 100])
                in_progress = len(df[(df['phys_complete_pct'] > 0) & (df['phys_complete_pct'] < 100)])
                not_started = len(df[df['phys_complete_pct'] == 0])

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Completed Tasks", completed)
                with col2:
                    st.metric("In Progress", in_progress)
                with col3:
                    st.metric("Not Started", not_started)

                # Show the data
                st.write("### Task Data")
                st.dataframe(df)

                # Add download buttons
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name="tasks.csv",
                    mime="text/csv"
                )

                # JSON for RAG
                json_data = df.to_json(orient='records', date_format='iso')
                st.download_button(
                    label="Download JSON (for RAG)",
                    data=json_data,
                    file_name="tasks.json",
                    mime="application/json"
                )
            else:
                st.error("No task data found in the file")

        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            import traceback
            st.code(traceback.format_exc())

        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    main()