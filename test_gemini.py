import os
from google import genai
os.getenv("GEMINI_API_KEY")
# The client gets the API key from the environment variable `GEMINI_API_KEY`.
content="Explain how AI works in a few words"



client = genai.Client()
response = client.models.generate_content(
    model="gemini-2.5-flash", contents=content
)
print(response.text)