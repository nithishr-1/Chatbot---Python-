import os
from dotenv import load_dotenv
import google.generativeai as genai
from difflib import get_close_matches


# Import model handlers
from model1 import process_user_query
from model2a import ask_data_question_about_segment
from model2b import ask_about_segment, load_segments, load_schema

# Load environment and configure LLM
load_dotenv()
genai.configure(api_key=os.getenv("google_api_key"))
top_level_model = genai.GenerativeModel("gemini-2.0-flash-lite")
sub_classifier_model = genai.GenerativeModel("gemini-2.0-flash-lite")

# Load segments and schema
segments = load_segments()
segment_names = list(segments.keys())
schema = load_schema()

# Classify top-level query: general vs segment
def classify_top_level(user_input, segment_names):
    segment_list = ", ".join(f'"{name}"' for name in segment_names)

    prompt = f"""
You are a classifier for a customer analytics system.

Your job is to classify user input into two categories:

1. `general` — if the user is asking about overall customer metrics, trends, or any other future trends in performance without referring to any specific segment.
   Examples:
   - "How many total customers do we have?"
   - "Show average price of all products in the 'Makeup' category."
   - "How many orders have been placed during december 2023"
   - "When this customer will most likely make a next purchase?"
   - "What product does this customer is most likely to buy?"

2. `segment` — if the user is asking about a specific segment or group of customers.
   These segments are defined dynamically from the user's customer data platform.

Current available segments:
{segment_list}

Return ONLY one word: `general` or `segment`.

User Input: "{user_input}"
"""
    response = top_level_model.generate_content(prompt)
    return response.text.strip().lower()


# Classify segment-based query into data vs strategy
def classify_segment_query(user_input):
    prompt = f"""
You are a classifier for a customer analytics chatbot. Your task is to classify user queries into two categories:

1. `data` — if the user is asking for metrics, numbers, or calculations about the segment.
2. `strategy` — if the user is asking for marketing advice, ideas, or strategic actions related to the segment.

Return ONLY one word: `data` or `strategy`.

User Input: "{user_input}"
"""
    response = sub_classifier_model.generate_content(prompt)
    return response.text.strip().lower()

from difflib import get_close_matches

# Detect and extract segment name from user query
def extract_segment_name_from_query(user_query, segment_names):
    for name in segment_names:
        if name.lower() in user_query.lower():
            return name
    # Fuzzy match fallback
    matches = get_close_matches(user_query, segment_names, n=1, cutoff=0.6)
    return matches[0] if matches else None

# Main flow

if __name__ == "__main__":
    active_segment = None

    while True:
        user_query = input("\nAsk your question (or type 'exit' to quit): ").strip()
        
        if user_query.lower() == "exit":
            break

        #  Classify at top level first
        query_type = classify_top_level(user_query, segment_names)

        if query_type == "general":
            #  Route to Model 1
            print("\n Routed to: Model 1 (General data question)\n")
            sql, output = process_user_query(user_query)
            print("\n SQL Used:\n", sql)
            print("\n Output:\n", output)
            continue

        elif query_type == "segment":
    # If the query contains a known segment, process it
            segment_in_query = extract_segment_name_from_query(user_query, segment_names)

            if not segment_in_query:
                print(" This is a segment-based question.")
                active_segment = input("Enter segment name: ").strip()
                segment_in_query = extract_segment_name_from_query(active_segment, segment_names)

                if not segment_in_query:
                    print("❌ Segment not found. Available segments are:")
                    for name in segment_names:
                        print("-", name)
                    continue

            # Segment found, enter follow-up loop
            print(f"\n Segment selected: '{segment_in_query}'")
            
            # Classify the original query immediately
            sub_type = classify_segment_query(user_query)

            if sub_type == "data":
                print("\n Routed to: Model 2A (Segment-level data query)\n")
                sql, output = ask_data_question_about_segment(segment_in_query, user_query)
                print("\n SQL Used:\n", sql)
                print("\n Output:\n", output)

            elif sub_type == "strategy":
                print("\n Routed to: Model 2B (Segment-level strategic insights)\n")
                rules = segments[segment_in_query]["rules"]
                answer = ask_about_segment(segment_in_query, rules, schema, user_query)
                print("\nAnswer:\n", answer)

            else:
                print(" Couldn’t understand that question. Try again.")

            # Now enter follow-up loop
            print("\nYou can now ask more questions about this segment. Type 'exit' to switch.\n")
            while True:
                followup = input(f"[{segment_in_query}] → ").strip()


                if followup.lower() == "exit":
                    break

                sub_type = classify_segment_query(followup)

                if sub_type == "data":
                    print("\n Routed to: Model 2A (Segment-level data query)\n")
                    sql, output = ask_data_question_about_segment(segment_in_query, followup)
                    print("\n SQL Used:\n", sql)
                    print("\n Output:\n", output)

                elif sub_type == "strategy":
                    print("\n Routed to: Model 2B (Segment-level strategic insights)\n")
                    rules = segments[segment_in_query]["rules"]
                    answer = ask_about_segment(segment_in_query, rules, schema, followup)
                    print("\nAnswer:\n", answer)

                else:
                    print(" Couldn’t understand that question. Try again or type 'exit' to change segment.")

            continue  # Go back to main prompt after exiting segment loop


        # Fallback
        print(" I couldn't detect a segment name. Please enter a segment name or ask a general question.")





