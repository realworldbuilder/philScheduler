import streamlit as st
import pandas as pd
import tempfile
import os
from typing import Optional, Dict, List, Union
import json


class XerParser:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def _safe_float(self, value: Union[str, int, float], default: float = 0.0) -> float:
        """Safely convert value to float, handling empty strings and invalid values."""
        if not value:  # Handles empty string, None, etc.
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default

    def _describe_relationship_type(self, rel_type: str) -> str:
        """Convert relationship type code to descriptive text."""
        relationship_types = {
            'PR_FS': 'Finish-to-Start (must finish before successor can start)',
            'PR_SS': 'Start-to-Start (must start before successor can start)',
            'PR_FF': 'Finish-to-Finish (must finish before successor can finish)',
            'PR_SF': 'Start-to-Finish (must start before successor can finish)'
        }
        return relationship_types.get(rel_type, rel_type)

    def _describe_lag(self, lag: float) -> str:
        """Create human-readable description of lag time."""
        if not lag:
            return "No lag time"
        if lag > 0:
            return f"With {lag} hour(s) lag time after"
        return f"With {abs(lag)} hour(s) lead time before"

    def _process_relationships(self, task_id: str, relationships: Dict, task_lookup: Dict) -> Dict:
        """Process relationships to include rich context about predecessors and successors."""

        def enrich_relationship(related_task_id: str, rel_type: str, lag: float) -> Dict:
            related_task = task_lookup.get(related_task_id, {})
            return {
                "task_id": related_task_id,
                "task_name": related_task.get('task_name', 'Unknown Task'),
                "task_code": related_task.get('task_code', ''),
                "relationship_type": self._describe_relationship_type(rel_type),
                "lag": lag,
                "lag_description": self._describe_lag(lag),
                "dates": {
                    "start": related_task.get('target_start_date', ''),
                    "finish": related_task.get('target_end_date', '')
                }
            }

        processed = {
            "predecessors": [],
            "successors": [],
            "relationship_summary": ""
        }

        for pred in relationships.get('predecessors', []):
            processed['predecessors'].append(
                enrich_relationship(pred['task_id'], pred['type'], pred['lag'])
            )

        for succ in relationships.get('successors', []):
            processed['successors'].append(
                enrich_relationship(succ['task_id'], succ['type'], succ['lag'])
            )

        # Create natural language summary
        summary_parts = []
        if processed['predecessors']:
            pred_names = [p['task_name'] for p in processed['predecessors']]
            summary_parts.append(f"This task must follow: {', '.join(pred_names)}")

        if processed['successors']:
            succ_names = [s['task_name'] for s in processed['successors']]
            summary_parts.append(f"This task is required before: {', '.join(succ_names)}")

        if not summary_parts:
            summary_parts.append("This task has no dependencies")

        processed['relationship_summary'] = ". ".join(summary_parts)

        return processed

    def parse_tables(self) -> Dict:
        """Parse all relevant tables for schedule analysis."""
        tables = {
            'TASK': [],
            'TASKPRED': [],
            'PROJWBS': [],
            'CALENDAR': [],
            'PROJECT': []
        }
        current_table = None
        fields = None

        with open(self.file_path, 'r', encoding='windows-1252') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                if line.startswith('%T'):
                    current_table = line[3:].strip()
                    fields = None

                elif line.startswith('%F') and current_table in tables:
                    fields = line[3:].split('\t')

                elif line.startswith('%R') and current_table in tables and fields:
                    values = line[3:].split('\t')
                    if len(values) < len(fields):
                        values.append('')
                    row_data = dict(zip(fields, values))
                    tables[current_table].append(row_data)

        return tables

    def process_for_rag(self) -> Dict:
        """Process XER data into LLM-friendly format."""
        tables = self.parse_tables()

        # Create task lookup for relationship processing
        task_lookup = {
            task['task_id']: task
            for task in tables.get('TASK', [])
        }

        # Process relationships
        relationships = {}
        for rel in tables.get('TASKPRED', []):
            if rel['task_id'] not in relationships:
                relationships[rel['task_id']] = {'predecessors': [], 'successors': []}
            if rel['pred_task_id'] not in relationships:
                relationships[rel['pred_task_id']] = {'predecessors': [], 'successors': []}

            relationships[rel['task_id']]['predecessors'].append({
                'task_id': rel['pred_task_id'],
                'type': rel.get('pred_type', ''),
                'lag': self._safe_float(rel.get('lag_hr_cnt', 0))
            })

            relationships[rel['pred_task_id']]['successors'].append({
                'task_id': rel['task_id'],
                'type': rel.get('pred_type', ''),
                'lag': self._safe_float(rel.get('lag_hr_cnt', 0))
            })

        # Create enhanced task data
        enhanced_tasks = []
        for task in tables.get('TASK', []):
            task_data = {
                'task_id': task['task_id'],
                'name': task.get('task_name', ''),
                'code': task.get('task_code', ''),
                'status': {
                    'code': task.get('status_code', ''),
                    'percent_complete': self._safe_float(task.get('phys_complete_pct', 0)),
                    'status_description': self._get_status_description(task)
                },
                'dates': {
                    'start': {
                        'target': task.get('target_start_date', ''),
                        'actual': task.get('act_start_date', ''),
                        'early': task.get('early_start_date', ''),
                        'late': task.get('late_start_date', '')
                    },
                    'finish': {
                        'target': task.get('target_end_date', ''),
                        'actual': task.get('act_end_date', ''),
                        'early': task.get('early_end_date', ''),
                        'late': task.get('late_end_date', '')
                    }
                },
                'duration': {
                    'target': self._safe_float(task.get('target_drtn_hr_cnt', 0)),
                    'remaining': self._safe_float(task.get('remain_drtn_hr_cnt', 0))
                },
                'float': {
                    'total': self._safe_float(task.get('total_float_hr_cnt', 0)),
                    'free': self._safe_float(task.get('free_float_hr_cnt', 0))
                },
                'relationships': self._process_relationships(
                    task['task_id'],
                    relationships.get(task['task_id'], {}),
                    task_lookup
                ),
                'natural_language_description': self._generate_task_description(task)
            }
            enhanced_tasks.append(task_data)

        project_data = tables.get('PROJECT', [{}])[0]

        return {
            'project_info': {
                'name': project_data.get('proj_short_name', 'Unknown Project'),
                'id': project_data.get('proj_id', ''),
                'start_date': project_data.get('start_date', ''),
                'finish_date': project_data.get('finish_date', ''),
                'data_date': project_data.get('last_recalc_date', '')
            },
            'tasks': enhanced_tasks,
            'schedule_metrics': self._calculate_schedule_metrics(enhanced_tasks),
            'critical_path_summary': self._identify_critical_path(enhanced_tasks)
        }

    def _get_status_description(self, task: Dict) -> str:
        """Generate human-readable status description."""
        status = task.get('status_code', '')
        percent = self._safe_float(task.get('phys_complete_pct', 0))

        if percent == 100:
            return "This task is completed"
        elif percent > 0:
            return f"This task is in progress, {percent}% complete"
        elif status == 'TK_NotStart':
            return "This task has not started yet"
        return "Status unknown"

    def _generate_task_description(self, task: Dict) -> str:
        """Generate natural language description of task."""
        status = self._get_status_description(task)
        duration = self._safe_float(task.get('target_drtn_hr_cnt', 0))

        description = f"Task '{task.get('task_name', '')}' (ID: {task.get('task_code', '')}) "
        description += f"is planned to take {duration} hours. {status}. "

        if task.get('target_start_date'):
            description += f"It is scheduled to start on {task['target_start_date']}. "

        if self._safe_float(task.get('total_float_hr_cnt', 0)) <= 0:
            description += "This is a critical task with no float. "

        return description.strip()

    def _calculate_schedule_metrics(self, tasks: List[Dict]) -> Dict:
        """Calculate high-level schedule metrics."""
        total_tasks = len(tasks)
        completed_tasks = sum(1 for task in tasks if task['status']['percent_complete'] == 100)
        in_progress = sum(1 for task in tasks
                          if 0 < task['status']['percent_complete'] < 100)

        return {
            'total_tasks': total_tasks,
            'completed_tasks': completed_tasks,
            'in_progress_tasks': in_progress,
            'not_started_tasks': total_tasks - completed_tasks - in_progress,
            'percent_complete': round(completed_tasks / total_tasks * 100 if total_tasks > 0 else 0, 2)
        }

    def _identify_critical_path(self, tasks: List[Dict]) -> List[Dict]:
        """Identify critical path tasks."""
        critical_tasks = [task for task in tasks
                          if task['float']['total'] <= 0]
        return sorted(critical_tasks,
                      key=lambda x: x['dates']['start']['target'] or '')


def main():
    st.title("Enhanced Schedule Data Extractor for RAG")
    st.write("Upload your P6 XER file to extract LLM-friendly schedule data")

    uploaded_file = st.file_uploader("Choose an XER file", type=['xer'])

    if uploaded_file is not None:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xer', mode='wb') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            temp_path = tmp_file.name

        try:
            parser = XerParser(temp_path)
            rag_data = parser.process_for_rag()

            # Display summary
            st.write("### Schedule Summary")
            metrics = rag_data['schedule_metrics']

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Tasks", metrics['total_tasks'])
            with col2:
                st.metric("Completed", metrics['completed_tasks'])
            with col3:
                st.metric("% Complete", f"{metrics['percent_complete']}%")

            # Display critical path summary
            st.write("### Critical Path Summary")
            critical_tasks = rag_data['critical_path_summary']
            if critical_tasks:
                st.write(f"Found {len(critical_tasks)} tasks on the critical path")

                # Show relationship examples
                st.write("### Sample Task Dependencies")
                sample_task = critical_tasks[0]
                st.write(f"Task: {sample_task['name']}")
                st.write("Relationship Summary:", sample_task['relationships']['relationship_summary'])

            # Export options
            st.write("### Export Options")

            # RAG JSON Export
            json_data = json.dumps(rag_data, indent=2)
            st.download_button(
                label="Download RAG JSON",
                data=json_data,
                file_name="schedule_rag.json",
                mime="application/json"
            )

            # Preview RAG data
            st.write("### RAG Data Preview")
            st.json(rag_data)

        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            import traceback
            st.code(traceback.format_exc())

        finally:
            os.unlink(temp_path)


if __name__ == "__main__":
    main()