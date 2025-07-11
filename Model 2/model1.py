def process_user_query(user_query):
    import os
    import re
    import json
    import csv
    import psycopg2
    import pandas as pd
    from langchain.prompts import FewShotPromptTemplate, PromptTemplate
    from langchain_google_genai import ChatGoogleGenerativeAI

    llm = ChatGoogleGenerativeAI(
        model="gemini-2.0-flash-lite",
        google_api_key="AIzaSyCKpban5zBotnx7D2EdtaEtwmAbWz5Fxck",
        temperature=0.0
    )

    # Load schema and prompt examplesquery
    with open("C:/Users/nithi/OneDrive/Documents/Datagen Tables/TablesinDatagen_schema.json", "r") as f:
        schema = json.load(f)

    with open("C:/Users/nithi/OneDrive/Documents/Datagen Tables/JOINprompts.json", "r") as f:
        joinprompt = json.load(f)

    # Few-shot SQL generation
    example_prompt = PromptTemplate(
        input_variables=["user_query", "sql_query"],
        template="User Query: {user_query}\nSQL Query: {sql_query}\n"
    )

    few_shot_prompt = FewShotPromptTemplate(
        examples=joinprompt,
        example_prompt=example_prompt,
        prefix="""
You are an expert SQL query generator. Learn from the examples below.

Important rules to MUST follow 
- ❗**VERY IMPORTANT: Always return the average of the value if the user asks in general. For example, if the user asks for "what is the click rate of the customers" then return the average of the click rate of all customers instead of showing it for an individual.**
- ❗**VERY IMPORTANT: While performing JOINS, use the common fields that are present in both the tables. Don't assume or invent on your own.**
- ❗Check if the asked field is there in any table using the schema. If it is there then include and extract that field from that table. Don't create any field names on your own. 
- ❗For columns with enumerated values (allowed_values), use ONLY the values exactly as listed in the schema (case-sensitive match). E.g., use `Click`, not `clicked`.
- ❗**VERY IMPORTANT: ONLY provide the SQL query. DO NOT include any explanations, comments, or markdown formatting (like ```sql).**
- ❗**VERY IMPORTANT: Always generate the field names in lowercase.

Here are some examples:""",
        suffix="""
Schema:
{schema}

User Query: {query}
SQL Query:""",
        input_variables=["schema", "query"]
    )

    chain = few_shot_prompt | llm
    response = chain.invoke({
        "schema": schema,
        "query": user_query
    })

    def extract_sql_only(text):
        match = re.search(r"\b(SELECT|WITH|INSERT|UPDATE|DELETE)\b", text, re.IGNORECASE)
        if match:
            return text[match.start():].strip()
        return text.strip()

    generated_sql = extract_sql_only(response.content)

    # Run SQL
    def run_query(query: str):
        conn = psycopg2.connect(
            host="localhost",
            user="postgres",         
            password="Datagets.ai",   
            dbname="datagentables"
        )
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            if cursor.description is None:
                raise Exception("Query returned no result set.")
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return columns, results
        except Exception as e:
            return [], f"❌ SQL Error: {e}"
        finally:
            cursor.close()
            conn.close()


    columns, results = run_query(generated_sql)

    # Format schema for validation
    with open("C:/Users/nithi/OneDrive/Documents/Datagen Tables/formatted_schema.json", "r") as f:
        schema_data = json.load(f)

    def format_schema_for_prompt(schema_data):
        lines = []
        for table_entry in schema_data:
            table_name = table_entry["table_name"]
            description = table_entry["description"]
            columns = table_entry["columns"]
            lines.append(f"Table: {table_name}")
            lines.append(f"Description: {description}")
            for column, desc in columns.items():
                lines.append(f"  - `{column}`: {desc}")
            lines.append("")
        return "\n".join(lines)

    schema_text = format_schema_for_prompt(schema_data)

    if not columns:
        print(results)
        validation_result = "No"
    else:
        df = pd.DataFrame(results, columns=columns)
        preview_data = df.head(5).to_string(index=False)

        validation_prompt = PromptTemplate(
            input_variables=["user_query", "sql_query", "sql_output", "schema"],
            template="""
You are an expert SQL validator. Your goal is to verify if the SQL query matches the user's request based on the schema and output.

Schema:
{schema}

User Query:
{user_query}

SQL Query:
{sql_query}

SQL Output Preview (Top 5 Rows):
{sql_output}

Instructions:
- Only say "No" if the SQL query is **logically incorrect** or **clearly does not match** the user query.
- Do **not** assume the user wants filtering, aggregation, grouping, or customer names **unless the user explicitly says so**.
- A column called `Response` represents a customer's response.
- The query is valid if it returns the response, campaign name, and channel, as the user asked.
- If the structure and output make sense for the given user query, answer "Yes".

Answer format:
Yes or No  
Explanation:
"""
        )
        validator_chain = validation_prompt | llm
        validation_response = validator_chain.invoke({
            "user_query": user_query,
            "sql_query": generated_sql,
            "sql_output": preview_data,
            "schema": schema_text
        })

        response_text = validation_response.content
        print(" LLM Validation:\n", response_text)
        first_line = response_text.strip().splitlines()[0].strip().lower()
        validation_result = "Yes" if first_line == "yes" else "No"

        if validation_result == "Yes":
            print(df.head(10))
        else:
            print("Validation failed, but here's the output from MySQL:")
            print(df.head(10))

    # Log to CSV
    def log_to_csv(user_query, generated_sql, validation_result, file_path='C:/Users/nithi/OneDrive/Documents/Datagen Tables/testcase_logger.csv'):
        file_exists = os.path.isfile(file_path)
        try:
            with open(file_path, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                if not file_exists:
                    writer.writerow(['User Query', 'Generated SQL', 'Validation Result'])
                writer.writerow([user_query, generated_sql, validation_result])
            print(f"✅ Successfully logged data to {file_path}") 
        except Exception as e:
            print(f"❌ Error logging to CSV: {e}")

    log_to_csv(user_query, generated_sql, validation_result)
    return generated_sql, results

if __name__ == "__main__":
    test_query = input("Enter your user query: ")
    sql_query, result = process_user_query(test_query)

    print("\n Final Generated SQL Query:\n", sql_query)