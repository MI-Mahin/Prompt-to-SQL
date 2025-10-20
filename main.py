import os
import mysql.connector
from dotenv import load_dotenv
import requests
import json
import sqlparse

# Load environment variables
load_dotenv()

class PromptToSQLConverter:
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
        
        # Try to find available model
        self.model_name = self.get_available_model()
        print(f"Using model: {self.model_name}")
        
        # Gemini API endpoint
        self.api_url = f"https://generativelanguage.googleapis.com/v1/models/{self.model_name}:generateContent?key={self.api_key}"
        
        self.schema_info = self.get_database_schema()
    
    def get_available_model(self):
        """Get list of available models and return the best one for text generation"""
        list_url = f"https://generativelanguage.googleapis.com/v1/models?key={self.api_key}"
        
        try:
            response = requests.get(list_url, timeout=10)
            if response.status_code == 200:
                models_data = response.json()
                if 'models' in models_data:
                    # Look for models that support generateContent
                    preferred_models = [
                        'gemini-1.5-flash',
                        'gemini-1.5-pro', 
                        'gemini-1.5-flash-latest',
                        'gemini-pro',
                        'gemini-1.0-pro'
                    ]
                    
                    available_models = []
                    for model in models_data['models']:
                        model_name = model.get('name', '').replace('models/', '')
                        supported_methods = model.get('supportedGenerationMethods', [])
                        if 'generateContent' in supported_methods:
                            available_models.append(model_name)
                    
                    # Try to find preferred model
                    for preferred in preferred_models:
                        for available in available_models:
                            if preferred in available:
                                return available
                    
                    # If no preferred found, return first available
                    if available_models:
                        return available_models[0]
        except Exception as e:
            print(f"Warning: Could not fetch models list: {e}")
        
        # Default fallback
        return 'gemini-1.5-flash'
    
    def get_database_schema(self):
        """Retrieve database schema information"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            schema = []
            
            # Get all tables
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                schema.append(f"\nTable: {table_name}")
                
                # Get columns for each table
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
        """Convert natural language prompt to SQL query using Gemini API"""
        system_instruction = f"""You are a MySQL query generator. Convert natural language questions into valid MySQL queries.

Database Schema:
{self.schema_info}

CRITICAL RULES:
1. Generate ONLY the SQL query text, absolutely nothing else
2. Use proper MySQL syntax
3. Return only SELECT statements for safety
4. Use proper table and column names from the schema above
5. Do NOT use markdown formatting, code blocks, or any special characters
6. Do NOT add explanations, comments, or any text before or after the query
7. Do NOT include semicolon at the end
8. Return ONLY the raw SQL query

User Question: {prompt}

Generate only the SQL query:"""

        payload = {
            "contents": [{
                "parts": [{
                    "text": system_instruction
                }]
            }],
            "generationConfig": {
                "temperature": 0,
                "maxOutputTokens": 500,
            }
        }

        try:
            response = requests.post(
                self.api_url,
                headers={'Content-Type': 'application/json'},
                json=payload,
                timeout=30
            )
            
            if response.status_code != 200:
                return f"API Error {response.status_code}: {response.text}"
            
            result = response.json()
            
            # Extract the generated text
            if 'candidates' in result and len(result['candidates']) > 0:
                sql_query = result['candidates'][0]['content']['parts'][0]['text'].strip()
            else:
                return "Error: No response generated"
            
            # Clean up the query
            sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
            sql_query = sql_query.strip('"\'')
            
            # Remove any text before SELECT
            if 'SELECT' in sql_query.upper():
                select_index = sql_query.upper().find('SELECT')
                sql_query = sql_query[select_index:]
            
            # Remove semicolon at the end
            sql_query = sql_query.rstrip(';').strip()
            
            return sql_query
        
        except requests.exceptions.RequestException as e:
            return f"Network error: {str(e)}"
        except Exception as e:
            return f"Error generating query: {str(e)}"
    
    def execute_query(self, sql_query):
        """Execute the generated SQL query"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute(sql_query)
            results = cursor.fetchall()
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            cursor.close()
            conn.close()
            
            return column_names, results
        
        except mysql.connector.Error as err:
            return None, f"Error executing query: {err}"
    
    def format_results(self, column_names, results):
        """Format query results for display"""
        if not results:
            return "No results found."
        
        # Calculate column widths
        widths = [len(name) for name in column_names]
        for row in results:
            for i, val in enumerate(row):
                widths[i] = max(widths[i], len(str(val)))
        
        # Create header
        header = " | ".join(name.ljust(widths[i]) for i, name in enumerate(column_names))
        separator = "-+-".join("-" * width for width in widths)
        
        # Create rows
        rows = []
        for row in results:
            rows.append(" | ".join(str(val).ljust(widths[i]) for i, val in enumerate(row)))
        
        return f"\n{header}\n{separator}\n" + "\n".join(rows)


def main():
    """Main function to run the prompt-to-SQL converter"""
    print("=" * 70)
    print("PROMPT TO SQL QUERY GENERATOR - FEASIBILITY TEST")
    print("Using Google Gemini API (Direct REST)")
    print("=" * 70)
    
    try:
        converter = PromptToSQLConverter()
    except Exception as e:
        print(f"\nError initializing converter: {e}")
        print("\nPlease check:")
        print("1. Your GEMINI_API_KEY is set correctly in .env file")
        print("2. MySQL is running and credentials are correct")
        print("3. Get your API key from: https://aistudio.google.com/app/apikey")
        return
    
    print("\nDatabase Schema:")
    print(converter.schema_info)
    print("\n" + "=" * 70)
    
    print("\nExample questions you can ask:")
    print("  - Show all employees")
    print("  - Find employees earning more than 70000")
    print("  - List employees hired after 2019")
    print("  - Count employees in each department")
    print("  - Show the highest paid employee")
    print("  - Get average salary by department")
    print("=" * 70)
    
    while True:
        print("\nEnter your question (or 'quit' to exit):")
        prompt = input("> ").strip()
        
        if prompt.lower() in ['quit', 'exit', 'q']:
            print("Goodbye!")
            break
        
        if not prompt:
            continue
        
        # Generate SQL query
        print("\n[Generating SQL query...]")
        sql_query = converter.generate_sql_query(prompt)
        
        if sql_query.startswith("Error") or sql_query.startswith("API Error") or sql_query.startswith("Network error"):
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
        
        # Ask if user wants to execute
        execute = input("\nExecute this query? (y/n): ").strip().lower()
        
        if execute == 'y':
            print("\n[Executing query...]")
            column_names, results = converter.execute_query(sql_query)
            
            if column_names:
                print("\nQuery Results:")
                print(converter.format_results(column_names, results))
                print(f"\nTotal rows: {len(results)}")
            else:
                print(f"\n{results}")  # Error message
        
        print("\n" + "=" * 70)


if __name__ == "__main__":
    main()