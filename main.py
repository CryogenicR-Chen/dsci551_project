import openai
import os
import json
from sqlalchemy import create_engine, text, inspect
import cohere
from tabulate import tabulate
from google import genai

# cohere init
co = cohere.ClientV2("API-Cohere")


# gemini init
client_gemini = genai.Client(api_key="API-Gemini")


# openai init
oai_api_key = "API-GPT"
client_openapi = openai.OpenAI(api_key=oai_api_key)

# MySQL Server Info
DB_HOST = "IP"
DB_USER = "User"
DB_PASSWORD = "Password"
DB_NAME = "dbname"
# DB_NAME = "s"

# MySQL Connection URL
db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(db_url)


# Metadata cache file
METADATA_CACHE_FILE = "db_full_metadata.json"

def gemini_to_sql(nl_query: str, metadata: dict) -> str:
    prompt = get_prompt_for_sql(nl_query,metadata)
    try:
        response = client_gemini.models.generate_content(
            model="gemini-2.0-flash", contents=prompt
        )
        return response.text
    except Exception as e:
        print("Error from Gemini:", e)
        return None

def gemini_to_vote(nl_query: str, sql1: str, sql2: str, metadata: dict) -> list:
    prompt = get_prompt_for_vote(nl_query, sql1, sql2, metadata)
    try:
        response = client_gemini.models.generate_content(
            model="gemini-2.0-flash", contents=prompt
        )
        raw_text = response.text.strip().lower()
        votes = raw_text.split()
        if len(votes) == 2 and all(v in {"yes", "no"} for v in votes):
            return votes
        else:
            print("Unexpected vote format:", raw_text)
            return None
    except Exception as e:
        print("Error from Gemini:", e)
        return None


def cohere_to_sql(nl_query: str, metadata: dict) -> str:
    prompt = get_prompt_for_sql(nl_query,metadata)
    try:
        response = co.chat(
            model="command-r-plus",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.message.content[0].text.strip()
    except Exception as e:
        print("Error from Cohere:", e)
        return None

def cohere_to_vote(nl_query: str, sql1: str, sql2: str, metadata: dict) -> list:
    prompt = get_prompt_for_vote(nl_query, sql1, sql2, metadata)
    try:
        response = co.chat(
            model="command-r-plus",
            messages=[{"role": "user", "content": prompt}]
        )
        raw_text = response.message.content[0].text.strip()
        votes = raw_text.split()
        if len(votes) == 2 and all(v in {"yes", "no"} for v in votes):
            return votes
        else:
            print("Unexpected vote format:", raw_text)
            return None
    except Exception as e:
        print("Error voting from Cohere::", e)
        return None
def chatgpt_to_sql(nl_query: str, metadata: dict) -> str:
    prompt = get_prompt_for_sql(nl_query,metadata)

    try:

        response = client_openapi.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print("Error generating SQL from LLM:", e)
        return None

def pre_check(nl_query: str, metadata: dict) -> bool:


    prompt = f"""
    You are an SQL assistant with access to the following database schema.
    The metadata is provided in JSON format. Each key in the top-level dictionary is a database name.
    Each value is a dictionary where:
    - Keys are table names.
    - Values contain:
      - columns: a list of column (table attribute) definitions with name, type, nullable, and default. 
      - primary_keys: a list of primary key column names.
      - foreign_keys: a list of dictionaries with foreign key relationships.
      - create_statement: the raw CREATE TABLE statement.

    {json.dumps(metadata, indent=2)}

    Your task is to judge whether the following natural language input is a reasonable attempt to interact with a relational database.

    Valid inputs include, but are not limited to:
    - Requests to view table information (e.g. "show all tables", "list columns in X", "tell all databases", "switch to xx database")
    - Queries with partial structure like "show me first 3 records of xxxx", "join xxx and xxx using id", etc.
    - Inputs that mention attributes, fields, columns, rows, schema, or structure — these likely mean table metadata.

    If the input is mostly irrelevant or nonsensical (e.g. "hello there", "just testing", or gibberish or "asdfasdfa"), return "False <reason>".
    Make the evaluation lenient — if the input might be interpreted in a meaningful way by a human, consider it valid.
    Avoid false negatives.

    Respond with:
    - "True" — if the input could reasonably be interpreted as a database query.
    - "False <reason>" — if it is clearly not meaningful.

    Respond with exactly one line.

    User input:
    {nl_query}
    """
    try:
        response = client_openapi.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        answer = response.choices[0].message.content.strip()
        if answer.startswith("True"):
            return True
        reason = answer.split(" ", 1)[1] if " " in answer else "Unknown reason"

        if answer.startswith("False"):
            if reason.strip().lower() in {"<reason>", "reason"}:
                print("⚠️  GPT Validation Feedback: Input is invalid")
            else:
                print("⚠️  GPT Validation Feedback:", reason)
        return False
    except Exception as e:
        print("Error validating user input with GPT:", e)
        return False

def chatgpt_to_vote(nl_query: str, sql1: str, sql2: str, metadata: dict) -> str:
    prompt = get_prompt_for_vote(nl_query, sql1, sql2, metadata)
    try:

        response = client_openapi.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        raw_text = response.choices[0].message.content.strip()
        votes = raw_text.split()
        if len(votes) == 2 and all(v in {"yes", "no"} for v in votes):
            return votes
        else:
            print("Unexpected vote format:", raw_text)
            return None

    except Exception as e:
        print("Error voting from GPT:", e)
        return None
def get_full_database_metadata():
    """
    Retrieve full database schema including:
    - Table names
    - Column names and data types
    - Primary keys
    - Foreign keys
    - CREATE TABLE statements
    """
    try:
        inspector = inspect(engine)
        databases = inspector.get_schema_names()

        # Exclude system databases
        excluded_schemas = {"mysql", "performance_schema", "information_schema", "sys"}
        metadata = {}

        for db in databases:
            if db in excluded_schemas:
                continue  # Skip system databases

            metadata[db] = {}
            tables = inspector.get_table_names(schema=db)

            for table in tables:
                table_info = {}

                # Get column details
                columns = inspector.get_columns(table, schema=db) #51 line
                table_info["columns"] = [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col.get("nullable", True),
                        "default": col.get("default", None),
                    }
                    for col in columns
                ]

                # Get primary keys (use get_pk_constraint)
                pk_constraint = inspector.get_pk_constraint(table, schema=db)
                primary_keys = pk_constraint.get("constrained_columns", [])
                table_info["primary_keys"] = primary_keys if primary_keys else None

                # Get foreign keys
                fks = inspector.get_foreign_keys(table, schema=db)
                table_info["foreign_keys"] = [
                    {
                        "column": fk["constrained_columns"],
                        "references": fk["referred_table"],
                        "referenced_columns": fk["referred_columns"],
                    }
                    for fk in fks
                ] if fks else None

                # Get CREATE TABLE statement
                table_info["create_statement"] = get_create_table_statement(table, db)

                # Store table info
                metadata[db][table] = table_info

        # Save metadata cache

        with open(METADATA_CACHE_FILE, "w") as f:
            json.dump(metadata, f, indent=4)

        return metadata
    except Exception as e:
        print("Error retrieving database metadata:", e)
        return None


def get_create_table_statement(table_name, schema):
    """
    Get CREATE TABLE statement for a given table.
    """
    try:
        query = f"SHOW CREATE TABLE `{schema}`.`{table_name}`"
        with engine.connect() as connection:
            result = connection.execute(text(query)).fetchone()
            return result[1] if result else None
    except Exception as e:
        print(f"Error retrieving CREATE TABLE statement for {table_name}: {e}")
        return None


def load_cached_metadata():
    """
    Load cached metadata to avoid redundant queries.
    """
    if os.path.exists(METADATA_CACHE_FILE):
        with open(METADATA_CACHE_FILE, "r") as f:
            return json.load(f)
    return None


def get_prompt_for_sql(nl_query: str, metadata: dict) -> str:
    prompt = f"""
    You are an SQL expert with access to the following database schema:
    The following is the database metadata in JSON format. Each key in the top-level dictionary is a database name.
    Each value is a dictionary where:
    - Keys are table names.
    - Values contain:
      - columns: a list of column (table attribute) definitions with name, type, nullable, and default. 
      - primary_keys: a list of primary key column names.
      - foreign_keys: a list of dictionaries with foreign key relationships.
      - create_statement: the raw CREATE TABLE statement.
    {json.dumps(metadata, indent=2)}

    Convert the following natural language query into an SQL statement.
    - Ensure the SQL query is valid for MySQL.
    - Use correct table and column names.
    - If the user asks to show attributes, columns, schema, structure, or similar words, they likely mean the table definition. Ideal result would be DESCRIBE xxx
    - If a WHERE condition is required but missing, infer a reasonable condition.
    - Use correct data types and respect primary/foreign key relationships.
    - Generate **only** the SQL query without explanation.
    - the output should be in plain text! plain text! with out any format
    - Ensure the SQL query is valid for MySQL.
    Query:
    {nl_query}
    """
    return prompt


def get_prompt_for_vote(nl_query: str, sql1: str, sql2: str, metadata: dict) -> str:
    prompt = f"""
        You are an SQL expert. You will be given a natural language query, a database schema, and two candidate SQL queries.
        
        Your task is to evaluate whether each SQL query correctly answers the user's natural language query **based on the given schema**.
        
        Output exactly one line containing two answers: "yes" or "no" for each SQL query, separated by a space.
        - "yes" means the SQL is correct and valid for the query.
        - "no" means the SQL is incorrect or fails to satisfy the query intent.
        
        Only output the two words, nothing else.
        
        Database Schema:
        {json.dumps(metadata, indent=2)}
        
        Natural Language Query:
        {nl_query}
        
        SQL Query 1:
        {sql1}
        
        SQL Query 2:
        {sql2}
        """
    return prompt

def fast_voting(nl_query, metadata):
    sqls = {
        "gpt": chatgpt_to_sql(nl_query, metadata),
        "cohere": cohere_to_sql(nl_query, metadata),
        "gemini": gemini_to_sql(nl_query, metadata)
    }

    sql_gpt = sqls["gpt"]
    if not sql_gpt:
        return None

    # Get votes from Cohere and Gemini
    vote1 = cohere_to_vote(nl_query, sql_gpt, sqls["cohere"], metadata)
    vote2 = gemini_to_vote(nl_query, sql_gpt, sqls["gemini"], metadata)

    yes_votes = 0
    for vote in [vote1, vote2]:
        if vote and vote[0] == "yes":  # First vote is for sql_gpt
            yes_votes += 1

    if yes_votes >= 1:
        return sql_gpt

    return "Error the other two model vote no"


def base_voting(nl_query, metadata):
    sql_candidates = {
        "gpt": chatgpt_to_sql(nl_query, metadata),
        "cohere": cohere_to_sql(nl_query, metadata),
        "gemini": gemini_to_sql(nl_query, metadata)
    }
    votes = {"gpt": 0, "cohere": 0, "gemini": 0}
    cohere_verdicts = cohere_to_vote(nl_query, sql_candidates["gpt"], sql_candidates["gemini"], metadata)
    if cohere_verdicts:
        if cohere_verdicts[0] == "yes": votes["gpt"] += 1
        if cohere_verdicts[1] == "yes": votes["gemini"] += 1
    gemini_verdicts = gemini_to_vote(nl_query, sql_candidates["gpt"], sql_candidates["cohere"], metadata)
    if gemini_verdicts:
        if gemini_verdicts[0] == "yes": votes["gpt"] += 1
        if gemini_verdicts[1] == "yes": votes["cohere"] += 1
    gpt_verdicts = chatgpt_to_vote(nl_query, sql_candidates["cohere"], sql_candidates["gemini"], metadata)
    if gpt_verdicts:
        if gpt_verdicts[0] == "yes": votes["cohere"] += 1
        if gpt_verdicts[1] == "yes": votes["gemini"] += 1

    best_model = max(votes, key=votes.get)  # model with highest vote count
    best_sql = sql_candidates[best_model]
    if votes["gpt"] == votes[best_model] and best_model != "gpt":
        best_sql = sql_candidates["gpt"]
    return best_sql


def execute_sql(query: str):
    try:
        with engine.connect() as connection:
            trans = connection.begin()
            result = connection.execute(text(query))

            if query.strip().lower().startswith(("select", "describe")):
                return [dict(row) for row in result.mappings()]
            elif query.strip().lower().startswith("show"):
                column_name = list(result.keys())[0]
                return [{column_name: list(row.values())[0]} for row in result.mappings()]
            else:
                trans.commit()
                return [{"exec": str("")}]  # treat as success with no rows
    except Exception as e:
        print("Error executing SQL query:", e)
        return [{"error": str(e)}]

def limit_check(sql_query: str) -> bool:
    sql = sql_query.strip().lower()
    if sql.startswith("select") and "limit" not in sql:
        if "where" in sql or "group by" in sql:
            return True
        print("Query: " + sql)
        print("\u26a0\ufe0f  Your query is a query without a filter.")
        confirm = input("Do you want to continue anyway? (y/n): ").strip().lower()
        if confirm != 'y':
            return False
    return True


def main():
    print("Welcome to ChatDB CLI for MySQL!")

    # Try loading metadata from cache
    metadata = load_cached_metadata()
    if not metadata:
        print("Fetching database metadata...")
        metadata = get_full_database_metadata()
        if metadata:
            print("Database metadata retrieved and cached.")
        else:
            print("Failed to retrieve database metadata.")

    mode = None
    while mode not in {"1", "2", "3"}:
        print("\nSelect voting mode:")
        print("1. Base Voting (high accuracy, slower)")
        print("2. Fast Voting (faster, GPT-prioritized)")
        print("3. Simple Voting (simple, only GPT)")
        mode = input("Enter 1, 2 or 3 to select a mode: ").strip()

    if mode.strip().lower() in {"1", "base"}:
        mode = "base"
    elif mode.strip().lower() in {"2", "fast"}:
        mode = "fast"
    elif mode.strip().lower() in {"3", "simple"}:
        mode = "simple"
    while True:
        user_input = input("\nEnter your command: ")
        if user_input.lower() == 'exit':
            break
        elif user_input.strip().lower() in {"1", "base"}:
            mode = "base"
            print("Change mode to base mode")
            continue
        elif user_input.strip().lower() in {"2", "fast"}:
            mode = "fast"
            print("Change mode to fast mode")
            continue
        elif user_input.strip().lower() in {"3", "simple"}:
            mode = "simple"
            print("Change mode to simple mode")
            continue
        elif len(user_input) <= 3:
            print("Invalid input. Try again")
            continue
        if mode != "simple" and not pre_check(user_input, metadata):
            retry = input("Do you want to force execution anyway? (y/n): ").strip().lower()
            if retry != "y":
                continue


        MAX_RETRIES = 2  # in addition to the first attempt
        attempt = 0
        final_sql = None
        result = None
        is_limit = True
        while attempt <= MAX_RETRIES:
            if mode == 'base':
                final_sql = base_voting(user_input, metadata)
            elif mode == 'fast':
                final_sql = fast_voting(user_input, metadata)
            else:
                final_sql = chatgpt_to_sql(user_input,metadata)

            if final_sql:
                final_sql = final_sql.strip().replace("```sql", "").replace("```", "")
                is_limit = limit_check(final_sql)
                if not is_limit:
                    break

                result = execute_sql(final_sql)

                if result and isinstance(result, list) and "error" in result[0]:
                    error_message = result[0]["error"]
                    # user_input = f"{user_input}\n# ERROR last time: {error_message} try to figure it out and generate a new sql"
                    user_input = f"{user_input}\n# ERROR last time: {error_message} SQL last time: {final_sql} try to figure it out and generate a new sql"
                else:
                    break
            elif final_sql.strip().split()[0] == "Error":
                user_input = f"{user_input}\n# ERROR last time: {final_sql} try to figure it out and generate a new sql"
            attempt += 1
            print(f"Attempt {attempt} failed. Retrying...")

        if is_limit:
            if final_sql or "error" not in result[0]:
                print("\nGenerated SQL:", final_sql)
                if final_sql.strip().lower().startswith(("select", "show", "describe")):
                    print("\nQuery results:")
                    print(tabulate(result, headers="keys", tablefmt="grid"))
                else:
                    print("\nQuery executed successfully.")
            else:
                print("\nFailed to generate a correct SQL after 3 attempts. Please check your query.")



if __name__ == "__main__":
    main()

