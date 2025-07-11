import json
import psycopg2
import pandas as pd
import re
import os
from dotenv import load_dotenv
from langchain.prompts import FewShotPromptTemplate, PromptTemplate
from langchain_google_genai import ChatGoogleGenerativeAI

load_dotenv()
def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

# Load files
segment_rules = load_json("C:\\Users\\nithi\\OneDrive\\Documents\\Chatbot\\Model 2\\full_segment_rules.json")
schema = load_json("C:\\Users\\nithi\\OneDrive\\Documents\\Chatbot\\Model 2\\customers_profile_master_schema.json")
examples = load_json("C:\\Users\\nithi\\OneDrive\\Documents\\Chatbot\\Model 2\\Model2a_examples.json")

# Display-to-DB column map
display_to_db_col = {
    "Customer ID": "customerid",
    "State": "state",
    "DMA": "dma",
    "Loyalty Tier": "loyalty_tier",
    "Gender": "gender",
    "Marketing Opt-In": "marketing_opt_in",
    "CRM Status": "crm_status",
    "CLV Category": "clv_category",
    "Age Segment": "age_segment",
    "Income Level": "income_level",
    "Tenure Days": "tenure_days",
    "Marital Status": "marital_status",
    "Is Email Subscribed": "is_email_subscribed",
    "Is SMS Subscribed": "is_sms_subscribed",
    "Is Push Notification Subscribed": "is_push_notification_subscribed",
    "Is Web Notification Subscribed": "is_web_notification_subscribed",
    "Total Campaigns Received": "total_campaigns_received",
    "Total Campaigns Clicked": "total_campaigns_clicked",
    "Preferred Channel": "preferred_channel",
    "Preferred Categories Clicked": "preferred_categories_clicked",
    "Email Open Rate": "email_open_rate",
    "Email Click Rate": "email_click_rate",
    "Email Conversion Rate": "email_conversion_rate",
    "Push Open Rate": "push_open_rate",
    "Push Click Rate": "push_click_rate",
    "Push Conversion Rate": "push_conversion_rate",
    "SMS Open Rate": "sms_open_rate",
    "SMS Click Rate": "sms_click_rate",
    "SMS Conversion Rate": "sms_conversion_rate",
    "Website Open Rate": "website_open_rate",
    "Website Click Rate": "website_click_rate",
    "Website Conversion Rate": "website_conversion_rate",
    "Open Rate": "open_rate",
    "Click Rate": "click_rate",
    "Conversion Rate": "conversion_rate",
    "Desktop Visitor": "desktop_visitor",
    "Mobile Visitor": "mobile_visitor",
    "Website Purchase": "website_purchase",
    "Last Visit": "last_visit",
    "Visits Last 30 Days": "visits_last_30_days",
    "LTV": "ltv",
    "Add to Cart": "add_to_cart",
    "Recent Categories Visited": "recent_categories_visited",
    "Purchase Channel": "purchase_channel",
    "Top Categories": "top_categories",
    "Top Subcategories": "top_subcategories",
    "Pct SKUs Bought Discount": "pct_skus_bought_discount",
    "New Arrivals Purchased": "new_arrivals_purchased",
    "Top Purchased SKU": "top_purchased_sku",
    "Most Purchased SKU": "most_purchased_sku",
    "Last Purchased SKU": "last_purchased_sku",
    "CRM Influence Rate": "crm_influence_rate",
    "Total Orders": "total_orders",
    "Avg Order Value": "avg_order_value",
    "Total Revenue Generated": "total_revenue_generated",
    "Count of SKU": "count_of_sku",
    "Category Count": "category_count",
    "Bold Affinity": "bold_affinity",
    "Discount Affinity": "discount_affinity",
    "Eco Friendly Affinity": "eco_friendly_affinity",
    "Elegant Affinity": "elegant_affinity",
    "Exclusive Affinity": "exclusive_affinity",
    "Flash Sale Affinity": "flash_sale_affinity",
    "Generic Affinity": "generic_affinity",
    "Hot Affinity": "hot_affinity",
    "Limited Edition Affinity": "limited_edition_affinity",
    "Luxury Affinity": "luxury_affinity",
    "Minimalist Affinity": "minimalist_affinity",
    "New Arrival Affinity": "new_arrival_affinity",
    "Playful Affinity": "playful_affinity",
    "Rewards Affinity": "rewards_affinity",
    "Seasonal Affinity": "seasonal_affinity",
    "Sophisticated Affinity": "sophisticated_affinity",
    "Sustainability Affinity": "sustainability_affinity",
    "Trendy Affinity": "trendy_affinity",
    "Urban Affinity": "urban_affinity",
    "Vibrant Affinity": "vibrant_affinity",
    "Warm Inviting Affinity": "warm_inviting_affinity",
    "Youthful Affinity": "youthful_affinity",
    "First Time Buyers": "first_time_buyers",
    "Repeat Buyers": "repeat_buyers",
    "Trigger Campaign Recipient": "trigger_campaign_recipient",
    "Deal Seekers": "deal_seekers",
    "On the Fence Buyers": "on_the_fence_buyers",
    "Propensity to Buy": "propensity_to_buy",
    "Subscription Buyers": "subscription_buyers",
    "Ready to Buy Loyalist": "ready_to_buy_loyalist",
    "One Touch Converter": "one_touch_converter",
    "Slow Burner": "slow_burner",
    "Channel Sensitive SMS Shopper": "channel_sensitive_sms_shopper",
    "Is Category Loyalist": "is_category_loyalist",
    "Dormant User": "dormant_user",
    "Holiday Shopper": "holiday_shopper",
    "Multi Channel Performer": "multi_channel_performer",
    "Skimmers": "skimmers",
    "Dropper": "dropper",
    "Cart Builders Not Buyer": "cart_builders_not_buyer",
    "Fatigue Sensitive": "fatigue_sensitive",
    "Loyalist with Stale Engagement": "loyalist_with_stale_engagement",
    "Collection Binger": "collection_binger",
    "Last Min Buyer": "last_min_buyer",
    "Lapsed Buyers": "lapsed_buyers",
    "New User No Purchase": "new_user_no_purchase"
}


# Function to build WHERE clause from segment rules
def build_where_clause(rules):
    # Case-insensitive mapping
    field_map = {k.lower(): v for k, v in display_to_db_col.items()}
    clauses = []
    for rule in rules:
        field = rule["field"]
        # Optional: Validate against actual DB columns if needed
        if field not in display_to_db_col.values():
            raise ValueError(f"Unknown field: {field}")

        operator = rule["operator"]
        value = rule["value"]
        if isinstance(value, str):
            value = f"'{value}'"
        clauses.append(f'"{field}" {operator} {value}')
    return " AND ".join(clauses)

# Connect and get customer IDs
def get_customer_ids(where_clause):
    conn = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Datagets.ai",
        dbname="datagentables"
    )
    cursor = conn.cursor()
    query = f'SELECT customerid FROM customers_profile_master WHERE {where_clause}'
    cursor.execute(query)
    ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return ids


# Build LangChain prompt
def build_prompt_chain():
    example_prompt = PromptTemplate(
        input_variables=["user_query", "sql_query"],
        template="User Query: {user_query}\nSQL Query: {sql_query}\n",
    )
    prompt = FewShotPromptTemplate(
        examples=examples,
        example_prompt=example_prompt,
        prefix="""You are an expert SQL query generator...
Important rules to MUST follow 
- ❗**If the user asks about a numeric column (e.g., avg_order_value, sms_click_rate, email_open_rate, etc.) and does not explicitly request a breakdown or per-customer detail, always return a single aggregated value using AVG(column_name) over the selected customers. Do not select the column directly without aggregation, as that would return multiple rows (one per customer), which is confusing except for age_segment and zipcode column.**
- ❗**VERY IMPORTANT: While performing JOINS, use the common fields that are present in both the tables. Don't assume or invent on your own.**
- ❗Check if the asked field is there in any table using the schema. If it is there then include and extract that field from that table. Don't create any field names on your own. 
- ❗For columns with enumerated values (allowed_values), use ONLY the values exactly as listed in the schema (case-sensitive match). E.g., use `Click`, not `clicked`.
- ❗**VERY IMPORTANT: ONLY provide the SQL query. DO NOT include any explanations, comments, or markdown formatting (like ```sql).**
- ❗**VERY IMPORTANT: Always generate the field names in lowercase.""",
        suffix="\nSchema:\n{schema}\n\nUser Query: {query}\nSQL Query:",
        input_variables=["schema", "query"],
    )
    return prompt | ChatGoogleGenerativeAI(
        model="gemini-2.0-flash-lite",
        google_api_key=os.environ["GOOGLE_API_KEY"],
        temperature=0.0,
    )

# Inject customerid filter
def inject_customer_filter(sql, customer_ids):
    id_list = ", ".join(f"'{cid}'" for cid in customer_ids)
    filter_clause = f"customerid IN ({id_list})"

    if "customerid IN" in sql:
        sql = re.sub(r"customerid\s+IN\s*\([^)]+\)", filter_clause, sql, flags=re.IGNORECASE)
    elif "WHERE" in sql.upper():
        sql = re.sub(r"(WHERE\s+)", rf"\1{filter_clause} AND ", sql, flags=re.IGNORECASE)
    else:
        match = re.search(r"\b(GROUP BY|ORDER BY|LIMIT)\b", sql, re.IGNORECASE)
        if match:
            before = sql[:match.start()].strip()
            after = sql[match.start():].strip()
            sql = f"{before} WHERE {filter_clause} {after}"
        else:
            sql += f" WHERE {filter_clause}"
    return sql.strip().rstrip(";")

# Execute SQL
def run_sql(query):
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
            return [], "❌ SQL returned no result set."
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return columns, results
    except Exception as e:
        return [], f"❌ SQL Error: {e}"
    finally:
        cursor.close()
        conn.close()


# Clean SQL from model output
def extract_sql_only(text):
    # Remove markdown-style formatting
    text = text.strip().strip("`").strip()
    # Look for start of SQL
    match = re.search(r"\b(SELECT|WITH|INSERT|UPDATE|DELETE)\b", text, re.IGNORECASE)
    return text[match.start():].strip() if match else text


#  MAIN FUNCTION
def ask_data_question_about_segment(segment_name, user_query):
    if segment_name not in segment_rules:
        return None, f"❌ Segment '{segment_name}' not found. Available: {list(segment_rules.keys())}"

    rules = segment_rules[segment_name]["rules"]
    where_clause = build_where_clause(rules)
    customer_ids = get_customer_ids(where_clause)

    if not customer_ids:
        return None, f"❌ No customers found for segment: {segment_name}"

    if any(word in user_query.lower() for word in ["average", "mean", "median", "trend", "summary", "range", "bucket", "bin", "percentile", "min", "max"]):
        user_query += " (Give an aggregate answer instead of listing all rows.)"

    chain = build_prompt_chain()
    response = chain.invoke({"schema": schema, "query": user_query})
    sql = extract_sql_only(response.content)
    sql = sql.strip().rstrip(";")
    where_clause = build_where_clause(rules)
    if "WHERE" in sql.upper():
        final_sql = re.sub(r"(WHERE\s+)", rf"\1{where_clause} AND ", sql, flags=re.IGNORECASE)
    else:
        final_sql = f"{sql} WHERE {where_clause}"


    columns, results = run_sql(final_sql)

    if isinstance(results, str):  # Error string
        return {
        'sql': final_sql,
        'columns': [],
        'rows': [],
        'error': results
    }
    else:
        return {
            'sql': final_sql.replace('"', ''),
            'columns': columns,
            'rows': results
        }


# Example usage
if __name__ == "__main__":
    while True:
        seg = input("\nEnter segment name (or type 'exit' to quit): ")
        if seg.lower() == "exit":
            break

        if seg not in segment_rules:
            print(f"❌ Segment '{seg}' not found. Available segments:\n{list(segment_rules.keys())}")
            continue

        while True:
            q = input("\nWhat do you want to ask? (Type 'exit' to choose a different segment or quit): ")
            if q.lower() == "exit":
                break

            result = ask_data_question_about_segment(seg, q)
            print(json.dumps(result, indent=2))

            # Optional: save to file
            #with open("output.json", "w") as f:
            #   json.dump(result, f, indent=2)

