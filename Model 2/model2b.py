import json
import os
from dotenv import load_dotenv
import google.generativeai as genai

# Load environment variables
load_dotenv()
GEMINI_API_KEY = os.getenv("google_api_key")

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.0-flash-lite")

# Load segment rules
def load_segments(path="C:\\Users\\nithi\\OneDrive\\Documents\\Chatbot\\Model 2\\full_segment_rules.json"):
    with open(path, "r") as f:
        return json.load(f)

# Load schema
def load_schema(path="C:\\Users\\nithi\\OneDrive\\Documents\\Chatbot\\Model 2\\customers_profile_master_schema.json"):
    with open(path, "r") as f:
        return json.load(f)

# Extract column info from customers_profile_master
def get_customers_profile_columns(schema):
    for table in schema:
        if table.get("table_name") == "customers_profile_master":
            return table.get("columns", {})
    return {}

# Format rules into readable text
def rules_to_text(rules):
    return ", ".join([r.get("display", "") for r in rules])

# Generate insights for a segment
def generate_insights(segment_name, rules, schema):
    readable_rules = rules_to_text(rules)

    columns = get_customers_profile_columns(schema)
    column_descriptions = ", ".join([
        f"{field}: {desc}" for field, desc in columns.items()
    ])

    prompt = f"""
You are a senior CRM marketing strategist for a B2B e-commerce company.

You are analyzing a customer segment named: "{segment_name}" which is defined by the following logic:
{readable_rules}

Customer profile schema includes the following fields:
{column_descriptions}

Your task:
Generate exactly 5 marketing insights (no more than 160 characters each) that help a marketer understand and use this segment effectively.

Each insight must:
- Be highly relevant to the rules defining the segment.
- Use schema fields to make strategic and tactical recommendations.
- Be clear, beginner-friendly, and highly actionable.
- Avoid vague or generic tips.
- Avoid repeating the segment name.
- Help marketers make decisions on targeting, timing, personalization, or strategy.

Each of the 5 insights must be:
- Prefixed exactly by a number followed by a period and space (e.g., "1. ")
- Unique — do not repeat any point or number
- Avoid markdown (no asterisks, no bold, no italics)

The 5 insights should each address one of these areas:
1. Who this segment is – What defines these customers?
2. Opportunities – How marketers can use this segment.
3. Tactics – Specific actions to engage or convert them.
4. Driving engagement – Personalization, channels, timing, etc.
5. Additional recommendations – Improvements, monitoring, or expansion.

Format like this:
1. ...
2. ...
3. ...
4. ...
5. ...

Think like a strategist who understands segmentation, channel performance, fatigue, lifecycle campaigns, and LTV optimization.
Use schema fields that are most relevant.
"""
    try:
        response = model.generate_content(prompt)

        #  Handle both string and TextPart output safely
        if hasattr(response, "text"):
            return {"answer": [line.strip() for line in str(response.text).strip().split("\n") if line.strip()]}
        elif hasattr(response, "candidates") and hasattr(response.candidates[0], "text"):
            return response.candidates[0].text.strip()
        else:
            return "[Could not parse Gemini response]"
    except Exception as e:
        print(f"Error generating insights: {e}")
        return "[Could not generate insights due to model error]"

# Q&A about the segment
def ask_about_segment(segment_name, rules, schema, user_question):
    readable_rules = rules_to_text(rules)

    columns = get_customers_profile_columns(schema)
    column_descriptions = ", ".join([
        f"{field}: {col}" if isinstance(col, str) else f"{field} ({col.get('type', 'unknown')}): {col.get('description', '')}"
        for field, col in columns.items()
    ])

    context = f"""You are a B2B e-commerce CRM strategist.

Segment definition: {segment_name}, based on:
{readable_rules}

Schema Summary: {next((t['description'] for t in schema if t.get('table_name') == 'customers_profile_master'), '')}
columns = get_customers_profile_columns(schema)
Key Fields: {", ".join(columns.keys())}

You will answer follow-up questions about this segment.

Always:
- Ground your answers in the segment's rules and the schema fields.
- Be strategic and clear.
- Avoid repeating generic suggestions.
- Do not use Markdown formatting — no asterisks, bold, or italics.
- Use schema fields to justify or enhance your answer.
- For improvements, suggest refinements to rules that can increase engagement, LTV, or performance.
- Each point should be no more than 160 characters if you are giving a list.
- If the user asks how to improve rules, show the current rules, suggest enhancements using schema data, and explain why.

Each of the 5 insights must be:
- Prefixed exactly by a number followed by a period and space (e.g., "1. ")
- Unique — do not repeat any point or number
- Avoid markdown (no asterisks, no bold, no italics)

Format like this:
1. ...
2. ...
3. ...
4. ...
5. ...

Only give meaningful, personalized, and tactical suggestions.
"""
    try:
        response = model.generate_content(f"{context}\n\nUser question: {user_question}")
        if hasattr(response, "text"):
            return {"answer": [line.strip() for line in response.text.strip().split("\n") if line.strip()]}
        return "[Could not parse response]"
    except Exception as e:
        return f"[Model error: {e}]"

# Main application flow
def main():
    segments = load_segments()
    schema = load_schema()

    segment_keys = list(segments.keys())

    while True:
        segment_name = input("Enter the segment name: ").strip()
        if segment_name in segments:
            break
        print(f"\n The segment '{segment_name}' is not available.")
        print("\n Available segments:")
        for name in segment_keys:
            print(" -", name)
        print("\nPlease enter a valid segment name from the above list.\n")

    rules = segments[segment_name]["rules"]

    print(f"\nGenerating insights for segment: {segment_name}\n")
    insights = generate_insights(segment_name, rules, schema)
    print(json.dumps(insights, indent=2))

    while True:
        user_question = input("\nAsk a follow-up question about this segment (or type 'exit' to quit): ")
        if user_question.lower() == "exit":
            break
        answer = ask_about_segment(segment_name, rules, schema, user_question)
        print(json.dumps(answer, indent=2))


if __name__ == "__main__":
    main()
