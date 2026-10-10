"""
========================================================================================
BDA Practical 11: Scalable SMS Spam Dataset Generator
Scenario        : SecureComm Inc. - Telecom Spam Prevention Engine
Description     : Generates a realistic, synthetic SMS dataset for scalable spam 
                  detection demonstrations with PySpark and MLlib.
========================================================================================
"""

import os
import argparse
import random
import csv

def generate_sms_dataset(num_rows=20000, output_path="data/sms_spam.csv", seed=42):
    random.seed(seed)
    
    # -------------------------------------------------------------------------
    # Realistic Templates for Ham (Legitimate) and Spam SMS Messages
    # -------------------------------------------------------------------------
    ham_templates = [
        "Hey {name}, are we still meeting for lunch at {time}?",
        "Can you send me the {subject} lecture notes before tomorrow?",
        "Don't forget to pick up {item} on your way home.",
        "Thanks for your help with the {subject} assignment, really appreciate it!",
        "Call me when you get free, need to discuss the {subject} project.",
        "Your OTP for login to SecureBank is {otp}. Valid for 10 minutes. Do not share.",
        "Meeting scheduled at {time} in Conference Room {room}. Please be on time.",
        "Hey {name}, hope you are doing well. Let's catch up this weekend!",
        "I've submitted the project files to the portal. Please review when possible.",
        "The bus leaves at {time}, make sure you reach the terminal on time.",
        "Hi {name}, can you check if the server is running properly?",
        "Good morning! Just reminding you about our team sync at {time}.",
        "I am stuck in traffic, will reach in {min} minutes.",
        "Did you see the latest announcement regarding the exam schedule?",
        "Happy birthday {name}! Hope you have a wonderful day ahead!"
    ]

    spam_templates = [
        "URGENT! You have won a £{amount} cash prize or a brand new {device}! Call {phone} now to claim!",
        "Congratulations! Your mobile number won £{amount} in our weekly draw. Reply CLAIM to receive your reward.",
        "FREE MSG: Win a guaranteed {device}! Call {phone} right now. T&C apply.",
        "Your loan request of £{amount} has been APPROVED! Click {link} to disburse funds immediately.",
        "ALERT: Suspicious activity detected on your bank account. Verify identity at {link} to avoid suspension.",
        "Get cheap {product} with 80% discount! Limited time offer. Order now at {link}",
        "SIX Swatch watches giveaway! Text WATCH to {phone} to get yours free today!",
        "FINAL NOTICE: You have {amount} reward points expiring today. Redeem at {link}.",
        "Double your income working from home! Earn £{amount} per day. Text START to {phone}.",
        "Exclusive deal for VIP customer: Get free 5GB mobile data recharge today! Click {link}",
        "You have 1 unread message from a secret admirer! Call {phone} to listen now.",
        "CLAIM NOW! You are selected for £{amount} Amazon Gift Voucher. Click {link} to verify."
    ]

    names = ["Aarav", "Rohan", "Priya", "Ananya", "Rahul", "Sneha", "Vikram", "Neha", "Amit", "Kavya"]
    subjects = ["Big Data Analytics", "Machine Learning", "Database Systems", "Cloud Computing", "Software Engineering"]
    items = ["groceries", "milk", "documents", "the laptop charger", "dinner"]
    devices = ["iPhone 15 Pro", "PlayStation 5", "Samsung Galaxy S24", "MacBook Air", "iPad Pro"]
    products = ["medications", "designer shoes", "crypto mining course", "brand watches"]
    links = ["http://bit.ly/claim-win99", "http://secure-verify-update.com", "http://claim-prize-now.net", "http://bank-alert-security.org"]

    print(f"Generating synthetic SMS dataset with {num_rows:,} records (Seed: {seed})...")

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    ham_count = 0
    spam_count = 0

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["label", "message_text"])

        for i in range(num_rows):
            # ~85% Ham, ~15% Spam distribution (realistic for SMS telecom traffic)
            is_spam = random.random() < 0.15

            if is_spam:
                template = random.choice(spam_templates)
                msg = template.format(
                    amount=random.choice([500, 1000, 2000, 5000, 10000]),
                    device=random.choice(devices),
                    phone=f"0800{random.randint(100000, 999999)}",
                    link=random.choice(links),
                    product=random.choice(products)
                )
                label = "spam"
                spam_count += 1
            else:
                template = random.choice(ham_templates)
                msg = template.format(
                    name=random.choice(names),
                    time=f"{random.randint(1, 12)}:{random.choice(['00', '15', '30', '45'])} {'AM' if random.random() > 0.5 else 'PM'}",
                    subject=random.choice(subjects),
                    item=random.choice(items),
                    otp=random.randint(100000, 999999),
                    room=random.randint(101, 404),
                    min=random.randint(5, 30)
                )
                label = "ham"
                ham_count += 1

            writer.writerow([label, msg])

    print(f"Dataset successfully created at: {output_path}")
    print(f"Total Records : {num_rows:,}")
    print(f"Ham Count     : {ham_count:,} ({ham_count / num_rows * 100:.1f}%)")
    print(f"Spam Count    : {spam_count:,} ({spam_count / num_rows * 100:.1f}%)")
    print("-" * 60)
    print("Note: This dataset is designed for local laptop demonstrations (10k-50k rows).")
    print("For HDFS university cluster execution, scale to 5,000,000 rows using --rows 5000000.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic SMS Spam dataset.")
    parser.add_argument("--rows", type=int, default=20000, help="Number of SMS rows to generate (default: 20000)")
    parser.add_argument("--output", type=str, default="data/sms_spam.csv", help="Output path for CSV dataset")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    
    args = parser.parse_args()
    generate_sms_dataset(num_rows=args.rows, output_path=args.output, seed=args.seed)
