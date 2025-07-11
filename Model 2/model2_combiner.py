import os
import json
from dotenv import load_dotenv
import google.generativeai as genai

# Import both models
from model2a import ask_data_question_about_segment
from model2b import ask_about_segment, load_segments, load_schema

# Load env vars and Gemini
load_dotenv()
genai.configure(api_key=os.getenv("google_api_key"))
classifier_model = genai.GenerativeModel("gemini-2.0-flash-lite")

# Load segment rules and schema once
segments = load_segments()
schema = load_schema()

# LLM-based classification
def classify_query(user_input):
    prompt = f"""
You are a classifier for a customer analytics chatbot. Your task is to classify user queries into two categories:

1. **data** — if the user is asking for metrics, numbers, or calculations about the segment. These include questions like:
   - "How many customers are in this segment?"
   - "What is their average order value?"
   - "What's the conversion rate?"
   

2. **strategy** — if the user is asking for marketing advice, ideas, or insights on how to improve engagement, target this segment, or increase performance. These include questions like:
   - "How can we better engage this segment?"
   - "What should we do to convert them faster?"
   - "Give marketing suggestions for this group."
   - "What are the existing rules of this segment?"
   - "How can we leverage this segment for upselling?"

Return ONLY the word: `data` or `strategy`.

User Input: "{user_input}"
"""

    response = classifier_model.generate_content(prompt)
    return response.text.strip().lower()

# Main loop
if __name__ == "__main__":
    while True:
        seg = input("\nEnter segment name (or type 'exit' to quit): ")
        if seg.lower() == "exit":
            break
        if seg not in segments:
            print(f"❌ Segment '{seg}' not found. Available: {list(segments.keys())}")
            continue

        while True:
            q = input("\nWhat do you want to ask? (Type 'exit' to choose a different segment or quit): ")
            if q.lower() == "exit":
                break

            query_type = classify_query(q)
            if query_type == "data":
                output = ask_data_question_about_segment(seg, q)
                print("\n Output:\n", json.dumps(output, indent=2))
                #print("\n SQL Used:\n", output["sql"])
                #print("\n Output:\n", output["rows"])
            elif query_type == "strategy":
                rules = segments[seg]["rules"]
                answer= ask_about_segment(seg, rules, schema, q)
                print("\n Answer:\n", json.dumps(answer, indent=2))

            else:
                print(f"❌ Couldn't classify query type. LLM returned: {query_type}")
