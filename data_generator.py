import pandas as pd
import random
import os
from datetime import datetime, timedelta

# Define branches for each city
cities = {
    "Chennai": ["Guindy", "Anna Nagar", "Ambattur", "Mylapore", "Vadapalani"],
    "Bangalore": ["MG Road", "Koramangala", "Indiranagar", "Jayanagar", "Whitefield"],
    "Hyderabad": ["Banjara Hills", "Gachibowli", "Madhapur", "Hitech City", "Ameerpet"]
}

# Define product categories and payment modes
product_categories = ["Electronics", "Groceries", "Clothing", "Health"]
payment_modes = ["Debit", "Credit", "UPI", "Net Banking"]
status_options = ["Failed", "Successful"]

# Define number of entries per branch
entries_per_branch = 400

# Generate random dates
def generate_random_date(start_date="2023-01-01", end_date="2023-12-31"):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

# Create a central folder for all cities
output_folder = "./Bank_Transactions"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Create the data for each city and branch
for city, branches in cities.items():
    for branch in branches:
        branch_data = []  # Data specific to the current branch
        for _ in range(entries_per_branch):
            # Generate pseudo-random unique string for transaction/customer IDs
            transaction_id = os.urandom(16).hex()
            customer_id = os.urandom(16).hex()
            
            # Generate a random transaction amount between 10 and 1000
            amount = round(random.uniform(10, 1000), 2)
            
            # Generate a random date between the range
            date = generate_random_date().strftime("%Y-%m-%d")
            
            # Randomly choose status (Failed or Successful)
            status = random.choice(status_options)
            
            # Generate a branch ID (e.g., AMB453 for Ambattur branch)
            branch_id = f"{branch[:3].upper()}{random.randint(100, 999)}"
            
            # Randomly choose payment mode and product category
            payment_mode = random.choice(payment_modes)
            merchant_name = f"Merchant_{random.randint(1, 100)}"
            product_category = random.choice(product_categories)
            
            # Append the generated data for the branch
            branch_data.append({
                "Transaction ID": transaction_id,
                "Customer ID": customer_id,
                "Amount": amount,
                "Date": date,
                "Status": status,
                "Branch ID": branch_id,
                "Branch Name": branch,
                "City": city,
                "Payment Mode": payment_mode,
                "Merchant Name": merchant_name,
                "Product Category": product_category
            })
        
        # Convert the branch data to a pandas DataFrame
        branch_df = pd.DataFrame(branch_data)
        
        # Save the DataFrame to a CSV file inside the central folder with the name format "BranchName_City.csv"
        branch_file_name = f"{branch}_{city}.csv"
        branch_file_path = os.path.join(output_folder, branch_file_name)
        branch_df.to_csv(branch_file_path, index=False)
        
        # Print success message for each file generated
        print(f"CSV file for {branch} in {city} generated successfully.")
