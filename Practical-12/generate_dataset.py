"""
========================================================================================
BDA Practical 12: Superstore Sales Dataset Generator
Scenario        : Retail Analytics Executive Dashboard
Description     : Generates a realistic, multi-year Superstore Sales dataset
                  (5,000 records) with products, regions, customer segments,
                  order dates, sales, discounts, and profit data.
========================================================================================
"""

import os
import argparse
import random
import csv
from datetime import datetime, timedelta

def generate_superstore_dataset(num_rows=5000, output_path="data/superstore_sales.csv", seed=42):
    random.seed(seed)
    
    # -------------------------------------------------------------------------
    # Master Lists for Retail Dimensions
    # -------------------------------------------------------------------------
    segments = ["Consumer", "Corporate", "Home Office"]
    
    region_state_city = {
        "East": [
            ("New York", "New York City"),
            ("Pennsylvania", "Philadelphia"),
            ("Massachusetts", "Boston"),
            ("New Jersey", "Newark")
        ],
        "West": [
            ("California", "Los Angeles"),
            ("California", "San Francisco"),
            ("Washington", "Seattle"),
            ("Oregon", "Portland")
        ],
        "Central": [
            ("Illinois", "Chicago"),
            ("Texas", "Houston"),
            ("Texas", "Dallas"),
            ("Michigan", "Detroit")
        ],
        "South": [
            ("Florida", "Miami"),
            ("Georgia", "Atlanta"),
            ("North Carolina", "Charlotte"),
            ("Virginia", "Richmond")
        ]
    }

    category_subcat_products = {
        "Furniture": {
            "Chairs": ["Hon Executive Leather Chair", "HARMAN Ergonomic Swivel Chair", "Novimex Fabric Chair"],
            "Tables": ["Lesro Conference Table", "Bevis Wood Table", "Hon Executive Dining Table"],
            "Bookcases": ["Sauder Classic Bookcase", "Bush Somerset Bookcase", "Riverside Mahogany Bookcase"],
            "Furnishings": ["Ultra Steel Door Mat", "Rubbermaid Desk Lamp", "Eldon Accent Clock"]
        },
        "Office Supplies": {
            "Binders": ["GBC Heavy Duty Binder", "IBICO Ring Binder", "Avery Durable Binders"],
            "Paper": ["Xerox 1980 Color Paper", "Hammermill Copy Paper", "Southworth Fine Paper"],
            "Storage": ["Sterilite Stacking Bins", "Fellowes Storage Boxes", "Carina Utility Crates"],
            "Art": ["Boston School Scissors", "Sanford Colored Pencils", "Staples Gel Pens"],
            "Envelopes": ["Kraft Self-Seal Envelopes", "Jiffy Padded Mailers", "Clasp Envelopes"]
        },
        "Technology": {
            "Phones": ["Apple iPhone 14 Pro", "Samsung Galaxy S22", "Cisco VoIP Phone", "Motorola Edge"],
            "Accessories": ["Logitech Wireless Mouse", "SanDisk 128GB USB Flash", "Belkin Surge Protector"],
            "Machines": ["HP LaserJet Enterprise Printer", "Canon ImageCLASS Copier", "Zebra Label Printer"],
            "Copiers": ["Canon Advanced Digital Copier", "Sharp Digital Color Copier", "Brother All-in-One"]
        }
    }

    customers = [
        ("CG-12520", "Claire Gute"),
        ("DV-13045", "Darrin Van Huff"),
        ("SO-20335", "Sean O'Donnell"),
        ("BH-11710", "Brosina Hoffman"),
        ("AA-10315", "Alex Avila"),
        ("IM-15070", "Irene Maddox"),
        ("HP-14815", "Harold Pawlan"),
        ("PK-19075", "Pete Kriz"),
        ("AG-10270", "Alejandro Grove"),
        ("ZD-21925", "Zuschuss Donatelli"),
        ("KB-16585", "Ken Black"),
        ("SF-20065", "Sandra Flanagan"),
        ("EB-13870", "Emily Burns"),
        ("EH-13945", "Eric Hoffmann"),
        ("TB-21520", "Tracy Blumstein"),
        ("MA-17560", "Matt Abelman"),
        ("GH-14485", "Gene Hale"),
        ("SN-20710", "Suraj Nalin"),
        ("LC-16885", "Lena Cacioppo"),
        ("RA-19885", "Ruben Ausman")
    ]

    start_date = datetime(2021, 1, 1)
    end_date = datetime(2024, 12, 31)
    total_days = (end_date - start_date).days

    print(f"Generating synthetic Superstore Sales dataset with {num_rows:,} records...")

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Order ID", "Order Date", "Ship Date", "Customer ID", "Customer Name",
            "Segment", "Country", "City", "State", "Postal Code", "Region",
            "Product ID", "Category", "Sub-Category", "Product Name",
            "Sales", "Quantity", "Discount", "Profit"
        ])

        for i in range(1, num_rows + 1):
            # Date generation
            random_days = random.randint(0, total_days)
            order_date = start_date + timedelta(days=random_days)
            ship_date = order_date + timedelta(days=random.randint(1, 5))
            
            order_id = f"CA-{order_date.year}-{random.randint(100000, 999999)}"
            cust_id, cust_name = random.choice(customers)
            segment = random.choice(segments)

            # Region & Location
            region = random.choice(list(region_state_city.keys()))
            state, city = random.choice(region_state_city[region])
            postal_code = random.randint(10000, 99999)

            # Category & Product
            cat = random.choice(list(category_subcat_products.keys()))
            subcat = random.choice(list(category_subcat_products[cat].keys()))
            prod_name = random.choice(category_subcat_products[cat][subcat])
            prod_id = f"{cat[:3].upper()}-{subcat[:2].upper()}-{random.randint(10000000, 99999999)}"

            # Financial Calculations
            unit_price = round(random.uniform(10.0, 1200.0), 2)
            quantity = random.randint(1, 10)
            
            # Discount (0%, 10%, 20%, 30%, 50%)
            discount = random.choice([0.0, 0.0, 0.1, 0.15, 0.2, 0.3, 0.5])
            
            # Base Sales revenue
            raw_sales = unit_price * quantity
            sales = round(raw_sales * (1.0 - discount), 2)

            # Profit modeling (High discount items like Tables/Binders often result in negative profit / loss leaders)
            if discount >= 0.3 or subcat in ["Tables", "Bookcases", "Supplies"]:
                profit_margin = random.uniform(-0.35, -0.05) if discount >= 0.2 else random.uniform(0.05, 0.25)
            else:
                profit_margin = random.uniform(0.12, 0.45)
            
            profit = round(sales * profit_margin, 2)

            writer.writerow([
                order_id,
                order_date.strftime("%Y-%m-%d"),
                ship_date.strftime("%Y-%m-%d"),
                cust_id,
                cust_name,
                segment,
                "United States",
                city,
                state,
                postal_code,
                region,
                prod_id,
                cat,
                subcat,
                prod_name,
                sales,
                quantity,
                discount,
                profit
            ])

    print(f"Dataset successfully generated at: {output_path}")
    print(f"Total Rows Generated : {num_rows:,}")
    print(f"Date Range           : {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print("-" * 65)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic Superstore Sales dataset.")
    parser.add_argument("--rows", type=int, default=5000, help="Number of sales rows to generate (default: 5000)")
    parser.add_argument("--output", type=str, default="data/superstore_sales.csv", help="Output CSV path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    generate_superstore_dataset(num_rows=args.rows, output_path=args.output, seed=args.seed)
