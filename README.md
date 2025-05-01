# dsci551_project

ChatDB is a natural language interface (NLI) for MySQL databases, allowing you to interact with the database using simple English commands. It integrates multiple large language models (LLMs) — OpenAI GPT, Cohere, and Gemini — to generate, validate, and execute SQL queries automatically.

---

## 📦 Prerequisites

Before you run the code, make sure you have:

- **Python 3.12.10** installed  
- **MySQL server** installed 
- Accounts and **API keys** for:
  - **OpenAI GPT (paid account)** → [https://platform.openai.com](https://platform.openai.com)  
  - **Cohere** → [https://cohere.com](https://cohere.com)  
  - **Google Gemini (Generative AI)** → [https://makersuite.google.com](https://makersuite.google.com)

---

## 🔧 Configuration

In your code, update the following variables with your own credentials:

```python
# Cohere API key
co = cohere.ClientV2("API-Cohere")  # Replace "API-Cohere" with your actual Cohere API key

# Gemini API key
client_gemini = genai.Client(api_key="API-Gemini")  # Replace "API-Gemini" with your Gemini API key

# OpenAI GPT API key (paid account required)
oai_api_key = "API-GPT"  
client_openapi = openai.OpenAI(api_key=oai_api_key)  # Replace "API-GPT" with your OpenAI API key

# MySQL Database info
DB_HOST = "IP"             # Replace with your MySQL server IP or hostname
DB_USER = "User"           # Replace with your MySQL username
DB_PASSWORD = "Password"   # Replace with your MySQL password
DB_NAME = "dbname"         # Replace with your target MySQL database name

## 💡 requirements.txt
cohere==5.15.0
openai==1.76.2
protobuf==6.30.2
SQLAlchemy==2.0.38
tabulate==0.9.0
jsonschema==4.23.0
google-auth==2.39.0
google-genai==1.11.0
PyMySQL==1.1.1
pandas==2.2.3
numpy==2.1.3