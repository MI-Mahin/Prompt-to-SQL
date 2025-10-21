import os
import mysql.connector
from dotenv import load_dotenv
import requests
import json
import sqlparse
import pandas as pd
import numpy as np
from statistics import mean, median, stdev
from decimal import Decimal
from datetime import datetime
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px

# Load environment variables
load_dotenv()

class AdvancedPromptToSQL:
    def __init__(self):
        """Initialize database connection and Gemini API"""
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME')
        }
        
        self.api_key = os.getenv('GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        
        self.model_name = self.get_available_model()
        print(f"Using model: {self.model_name}")
        
        self.api_url = f"https://generativelanguage.googleapis.com/v1/models/{self.model_name}:generateContent?key={self.api_key}"
        
        self.schema_info = self.get_database_schema()
        
        # Store query results and history
        self.last_results = None
        self.last_columns = None
        self.last_query = None
        self.query_history = []
        
        # Create output directory for exports
        if not os.path.exists('outputs'):
            os.makedirs('outputs')
    
    def get_available_model(self):
        """Get available Gemini model"""
        list_url = f"https://generativelanguage.googleapis.com/v1/models?key={self.api_key}"
        
        try:
            response = requests.get(list_url, timeout=10)
            if response.status_code == 200:
                models_data = response.json()
                if 'models' in models_data:
                    preferred_models = ['gemini-1.5-flash', 'gemini-1.5-pro', 'gemini-pro']
                    
                    available_models = []
                    for model in models_data['models']:
                        model_name = model.get('name', '').replace('models/', '')
                        supported_methods = model.get('supportedGenerationMethods', [])
                        if 'generateContent' in supported_methods:
                            available_models.append(model_name)
                    
                    for preferred in preferred_models:
                        for available in available_models:
                            if preferred in available:
                                return available
                    
                    if available_models:
                        return available_models[0]
        except:
            pass
        
        return 'gemini-1.5-flash'
    
    def get_database_schema(self):
        """Retrieve database schema information"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            schema = []
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                schema.append(f"\nTable: {table_name}")
                cursor.execute(f"DESCRIBE {table_name}")
                columns = cursor.fetchall()
                
                for column in columns:
                    col_name, col_type, null, key, default, extra = column
                    schema.append(f"  - {col_name} ({col_type}){' PRIMARY KEY' if key == 'PRI' else ''}")
            
            cursor.close()
            conn.close()
            
            return "\n".join(schema)
        
        except mysql.connector.Error as err:
            return f"Error getting schema: {err}"
    
    def generate_sql_query(self, prompt):
        """Generate SQL query from natural language"""
        system_instruction = f"""You are a MySQL query generator. Convert natural language questions into valid MySQL queries.

Database Schema:
{self.schema_info}

RULES:
1. Generate ONLY the SQL query, no explanations
2. Use proper MySQL syntax
3. Return only SELECT statements for safety
4. Support JOIN operations for multi-table queries
5. Use aggregate functions (SUM, AVG, COUNT, etc.) when needed
6. Do NOT use markdown or code blocks
7. Do NOT include semicolon
8. Return ONLY the raw SQL query

User Question: {prompt}

SQL Query:"""

        payload = {
            "contents": [{"parts": [{"text": system_instruction}]}],
            "generationConfig": {"temperature": 0, "maxOutputTokens": 500}
        }

        try:
            response = requests.post(self.api_url, headers={'Content-Type': 'application/json'}, 
                                    json=payload, timeout=30)
            
            if response.status_code != 200:
                return f"API Error {response.status_code}: {response.text}"
            
            result = response.json()
            
            if 'candidates' in result and len(result['candidates']) > 0:
                sql_query = result['candidates'][0]['content']['parts'][0]['text'].strip()
            else:
                return "Error: No response generated"
            
            sql_query = sql_query.replace('```sql', '').replace('```', '').strip().strip('"\'')
            
            if 'SELECT' in sql_query.upper():
                select_index = sql_query.upper().find('SELECT')
                sql_query = sql_query[select_index:]
            
            return sql_query.rstrip(';').strip()
        
        except Exception as e:
            return f"Error generating query: {str(e)}"
    
    def execute_query(self, sql_query):
        """Execute SQL query and store results"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute(sql_query)
            results = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            
            cursor.close()
            conn.close()
            
            # Store results
            self.last_results = results
            self.last_columns = column_names
            self.last_query = sql_query
            self.query_history.append({
                'query': sql_query,
                'timestamp': datetime.now(),
                'rows': len(results)
            })
            
            return column_names, results
        
        except mysql.connector.Error as err:
            return None, f"Error executing query: {err}"
    
    def to_dataframe(self):
        """Convert last query results to pandas DataFrame"""
        if not self.last_results or not self.last_columns:
            return None
        
        return pd.DataFrame(self.last_results, columns=self.last_columns)
    
    # ==================== FEATURE 1: MULTI-TABLE CALCULATIONS ====================
    
    def multi_table_analysis(self):
        """Analyze data across multiple tables"""
        if not self.last_results:
            return "Error: Execute a query first"
        
        df = self.to_dataframe()
        
        analysis = {
            'total_rows': len(df),
            'columns': list(df.columns),
            'numeric_columns': list(df.select_dtypes(include=[np.number]).columns),
            'text_columns': list(df.select_dtypes(include=['object']).columns)
        }
        
        # Statistics for numeric columns
        if analysis['numeric_columns']:
            analysis['statistics'] = df[analysis['numeric_columns']].describe().to_dict()
        
        return analysis
    
    # ==================== FEATURE 2: PERCENTAGE CALCULATIONS ====================
    
    def percentage_analysis(self, column_name):
        """Calculate percentage distribution"""
        if not self.last_results:
            return "Error: Execute a query first"
        
        df = self.to_dataframe()
        
        if column_name not in df.columns:
            return f"Error: Column '{column_name}' not found"
        
        # Check if numeric
        if df[column_name].dtype in [np.number, 'int64', 'float64']:
            total = df[column_name].sum()
            df['percentage'] = (df[column_name] / total * 100).round(2)
            df['cumulative_percentage'] = df['percentage'].cumsum().round(2)
            
            return df
        else:
            # For categorical data, show distribution
            value_counts = df[column_name].value_counts()
            percentages = (value_counts / len(df) * 100).round(2)
            
            result_df = pd.DataFrame({
                'value': value_counts.index,
                'count': value_counts.values,
                'percentage': percentages.values
            })
            
            return result_df
    
    # ==================== FEATURE 3: TREND ANALYSIS ====================
    
    def trend_analysis(self, date_column, value_column):
        """Analyze trends over time"""
        if not self.last_results:
            return "Error: Execute a query first"
        
        df = self.to_dataframe()
        
        if date_column not in df.columns or value_column not in df.columns:
            return "Error: Specified columns not found"
        
        # Convert to datetime if not already
        try:
            df[date_column] = pd.to_datetime(df[date_column])
        except:
            return "Error: Date column cannot be converted to datetime"
        
        # Sort by date
        df = df.sort_values(date_column)
        
        # Calculate trend metrics
        analysis = {
            'start_date': df[date_column].min(),
            'end_date': df[date_column].max(),
            'total_records': len(df),
            'trend_data': df[[date_column, value_column]].to_dict('records')
        }
        
        # Calculate growth rate if numeric
        if df[value_column].dtype in [np.number, 'int64', 'float64']:
            first_value = df[value_column].iloc[0]
            last_value = df[value_column].iloc[-1]
            
            if first_value != 0:
                growth_rate = ((last_value - first_value) / first_value) * 100
                analysis['growth_rate'] = round(growth_rate, 2)
                analysis['first_value'] = float(first_value)
                analysis['last_value'] = float(last_value)
        
        return analysis
    
    # ==================== FEATURE 4: EXPORT TO CSV/EXCEL ====================
    
    def export_to_csv(self, filename=None):
        """Export results to CSV"""
        if not self.last_results:
            return "Error: No data to export"
        
        df = self.to_dataframe()
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"outputs/query_results_{timestamp}.csv"
        
        df.to_csv(filename, index=False)
        return f"Data exported to {filename}"
    
    def export_to_excel(self, filename=None, include_summary=True):
        """Export results to Excel with optional summary sheet"""
        if not self.last_results:
            return "Error: No data to export"
        
        df = self.to_dataframe()
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"outputs/query_results_{timestamp}.xlsx"
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            # Write main data
            df.to_excel(writer, sheet_name='Data', index=False)
            
            if include_summary:
                # Create summary sheet
                summary_data = []
                summary_data.append(['Query Executed', self.last_query])
                summary_data.append(['Timestamp', datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
                summary_data.append(['Total Rows', len(df)])
                summary_data.append(['Total Columns', len(df.columns)])
                summary_data.append(['', ''])
                summary_data.append(['Column Statistics', ''])
                
                summary_df = pd.DataFrame(summary_data, columns=['Metric', 'Value'])
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Add numeric column statistics
                numeric_df = df.select_dtypes(include=[np.number])
                if not numeric_df.empty:
                    stats_df = numeric_df.describe()
                    stats_df.to_excel(writer, sheet_name='Statistics')
        
        return f"Data exported to {filename}"
    
    # ==================== FEATURE 5: VISUALIZATION ====================
    
    def create_bar_chart(self, x_column, y_column, title=None):
        """Create bar chart visualization"""
        if not self.last_results:
            return "Error: Execute a query first"
        
        df = self.to_dataframe()
        
        if x_column not in df.columns or y_column not in df.columns:
            return "Error: Specified columns not found"
        
        fig = px.bar(df, x=x_column, y=y_column, 
                     title=title or f'{y_column} by {x_column}')
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"outputs/bar_chart_{timestamp}.html"
        fig.write_html(filename)
        
        return f"Bar chart saved to {filename}"
    
    def create_pie_chart(self, labels_column, values_column, title=None):
        """Create pie chart visualization"""
        if not self.last_results:
            return "Error: Execute a query first"
        
        df = self.to_dataframe()
        
        if labels_column not in df.columns or values_column not in df.columns:
            return "Error: Specified columns not found"
        
        fig = px.pie(df, names=labels_column, values=values_column,
                     title=title or f'{values_column} Distribution')
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"outputs/pie_chart_{timestamp}.html"
        fig.write_html(filename)
        
        return f"Pie chart saved to {filename}"
    
    def create_line_chart(self, x_column, y_column, title=None):
        """Create line chart for trend visualization"""
        if not self.last_results:
            return "Error: Execute a query first"
        
        df = self.to_dataframe()
        
        if x_column not in df.columns or y_column not in df.columns:
            return "Error: Specified columns not found"
        
        fig = px.line(df, x=x_column, y=y_column,
                      title=title or f'{y_column} Trend over {x_column}')
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"outputs/line_chart_{timestamp}.html"
        fig.write_html(filename)
        
        return f"Line chart saved to {filename}"
    
    def create_dashboard(self):
        """Create comprehensive dashboard with multiple visualizations"""
        if not self.last_results:
            return "Error: Execute a query first"
        
        df = self.to_dataframe()
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if len(numeric_cols) == 0:
            return "Error: No numeric columns for visualization"
        
        # Create subplots
        from plotly.subplots import make_subplots
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Distribution', 'Box Plot', 'Statistics Summary', 'Correlation')
        )
        
        # Add histogram
        if len(numeric_cols) > 0:
            fig.add_trace(
                go.Histogram(x=df[numeric_cols[0]], name=numeric_cols[0]),
                row=1, col=1
            )
        
        # Add box plot
        if len(numeric_cols) > 0:
            fig.add_trace(
                go.Box(y=df[numeric_cols[0]], name=numeric_cols[0]),
                row=1, col=2
            )
        
        fig.update_layout(height=800, showlegend=False, 
                         title_text="Data Analysis Dashboard")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"outputs/dashboard_{timestamp}.html"
        fig.write_html(filename)
        
        return f"Dashboard saved to {filename}"
    
    def format_results(self, column_names, results):
        """Format query results for display"""
        if not results:
            return "No results found."
        
        widths = [len(name) for name in column_names]
        for row in results:
            for i, val in enumerate(row):
                widths[i] = max(widths[i], len(str(val)))
        
        header = " | ".join(name.ljust(widths[i]) for i, name in enumerate(column_names))
        separator = "-+-".join("-" * width for width in widths)
        
        rows = []
        for row in results:
            rows.append(" | ".join(str(val).ljust(widths[i]) for i, val in enumerate(row)))
        
        return f"\n{header}\n{separator}\n" + "\n".join(rows)


def print_menu():
    """Display available commands"""
    print("\n" + "=" * 70)
    print("AVAILABLE COMMANDS:")
    print("=" * 70)
    print("Query Commands:")
    print("  - Ask any question (e.g., 'Show all employees')")
    print("  - 'history' - View query history")
    print("\nAnalysis Commands:")
    print("  - 'analyze' - Multi-table analysis")
    print("  - 'percentage' - Calculate percentage distribution")
    print("  - 'trend' - Analyze trends over time")
    print("\nExport Commands:")
    print("  - 'export csv' - Export to CSV")
    print("  - 'export excel' - Export to Excel")
    print("\nVisualization Commands:")
    print("  - 'viz bar' - Create bar chart")
    print("  - 'viz pie' - Create pie chart")
    print("  - 'viz line' - Create line chart")
    print("  - 'viz dashboard' - Create comprehensive dashboard")
    print("\nOther Commands:")
    print("  - 'menu' - Show this menu")
    print("  - 'quit' - Exit program")
    print("=" * 70)


def main():
    """Main function"""
    print("=" * 70)
    print("ADVANCED PROMPT TO SQL WITH FULL ANALYTICS")
    print("=" * 70)
    
    try:
        converter = AdvancedPromptToSQL()
    except Exception as e:
        print(f"\nError initializing: {e}")
        return
    
    print("\nDatabase Schema:")
    print(converter.schema_info)
    
    print_menu()
    
    while True:
        print("\nEnter command or question:")
        prompt = input("> ").strip()
        
        if prompt.lower() in ['quit', 'exit', 'q']:
            print("Goodbye!")
            break
        
        if not prompt:
            continue
        
        # Handle commands
        if prompt.lower() == 'menu':
            print_menu()
            continue
        
        if prompt.lower() == 'history':
            print("\nQuery History:")
            print("-" * 70)
            for i, entry in enumerate(converter.query_history, 1):
                print(f"{i}. [{entry['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}]")
                print(f"   Query: {entry['query']}")
                print(f"   Rows: {entry['rows']}")
            continue
        
        if prompt.lower() == 'analyze':
            result = converter.multi_table_analysis()
            print("\nMulti-Table Analysis:")
            print("-" * 70)
            print(json.dumps(result, indent=2, default=str))
            continue
        
        if prompt.lower() == 'percentage':
            print(f"Available columns: {', '.join(converter.last_columns)}")
            col = input("Enter column name: ").strip()
            result = converter.percentage_analysis(col)
            if isinstance(result, str):
                print(result)
            else:
                print("\nPercentage Analysis:")
                print(result.to_string())
            continue
        
        if prompt.lower() == 'trend':
            print(f"Available columns: {', '.join(converter.last_columns)}")
            date_col = input("Enter date column: ").strip()
            value_col = input("Enter value column: ").strip()
            result = converter.trend_analysis(date_col, value_col)
            print("\nTrend Analysis:")
            print(json.dumps(result, indent=2, default=str))
            continue
        
        if prompt.lower() == 'export csv':
            result = converter.export_to_csv()
            print(result)
            continue
        
        if prompt.lower() == 'export excel':
            result = converter.export_to_excel()
            print(result)
            continue
        
        if prompt.lower() == 'viz bar':
            print(f"Available columns: {', '.join(converter.last_columns)}")
            x_col = input("Enter X column: ").strip()
            y_col = input("Enter Y column: ").strip()
            result = converter.create_bar_chart(x_col, y_col)
            print(result)
            continue
        
        if prompt.lower() == 'viz pie':
            print(f"Available columns: {', '.join(converter.last_columns)}")
            label_col = input("Enter label column: ").strip()
            value_col = input("Enter value column: ").strip()
            result = converter.create_pie_chart(label_col, value_col)
            print(result)
            continue
        
        if prompt.lower() == 'viz line':
            print(f"Available columns: {', '.join(converter.last_columns)}")
            x_col = input("Enter X column: ").strip()
            y_col = input("Enter Y column: ").strip()
            result = converter.create_line_chart(x_col, y_col)
            print(result)
            continue
        
        if prompt.lower() == 'viz dashboard':
            result = converter.create_dashboard()
            print(result)
            continue
        
        # Generate and execute SQL query
        print("\n[Generating SQL query...]")
        sql_query = converter.generate_sql_query(prompt)
        
        if sql_query.startswith("Error"):
            print(f"\n{sql_query}")
            continue
        
        print("\nGenerated SQL Query:")
        print("-" * 70)
        try:
            formatted_sql = sqlparse.format(sql_query, reindent=True, keyword_case='upper')
            print(formatted_sql)
        except:
            print(sql_query)
        print("-" * 70)
        
        execute = input("\nExecute this query? (y/n): ").strip().lower()
        
        if execute == 'y':
            print("\n[Executing query...]")
            column_names, results = converter.execute_query(sql_query)
            
            if column_names:
                print("\nQuery Results:")
                print(converter.format_results(column_names, results))
                print(f"\nTotal rows: {len(results)}")
                print("\nTip: Use 'analyze', 'export', or 'viz' commands for further analysis")
            else:
                print(f"\n{results}")


if __name__ == "__main__":
    main()