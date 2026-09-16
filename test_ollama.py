import os
import requests
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("OLLAMA_HOST") + "/api/generate"

response = requests.post(
    url,
    json={
        "model": os.getenv("OLLAMA_MODEL"),
        "prompt": """
Write a PostgreSQL query to count distinct profile_id
from a table called onboarding_events.
Return only the SQL.
""",
        "stream": False,
    },
)

print(response.json()["response"])
