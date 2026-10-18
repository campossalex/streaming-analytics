# Purpose: Generates synthetic customer feedback events to the store.feedback Kafka topic.
#
# Models customer reviews for coffee shop purchases. Produces a mix of:
#   ~70% positive reviews (score 7–10) — populate the stream without triggering ML
#   ~15% neutral reviews  (score 5–6)  — near-misses, don't trigger ML_PREDICT
#   ~15% bad reviews      (score 1–4)  — trigger ML_PREDICT summarization in Flink
#
# Review text is template-based — varied enough to produce meaningful LLM summaries.
# Reads product list from ../data/products.csv (same file as purchases.py).

import configparser
import csv
import json
import os
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

# ── Configuration ──────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
config.read("configuration/configuration.ini")

BOOTSTRAP      = config["KAFKA"]["bootstrap_servers"]
TOPIC_FEEDBACK = config["KAFKA"]["topic_feedback"]

MIN_FREQ = int(config["FEEDBACK"]["min_feedback_freq"])
MAX_FREQ = int(config["FEEDBACK"]["max_feedback_freq"])

# ── Constants ──────────────────────────────────────────────────────────────────
US_STATES  = ["AZ", "CA", "CO", "ID", "MO", "NV", "NM", "OR", "UT", "WA", "WY"]
VISIT_TYPES = ["dine_in", "takeaway", "drive_through"]

# Review text templates per score band.
# Placeholders: {product}, {state}, {visit_type}, {issue}, {staff_adj}, {product_adj}
BAD_TEMPLATES = [
    "I ordered the {product} at the {state} location ({visit_type}) and it was {product_adj}. "
    "The staff was {staff_adj} and it took forever to get served. Very disappointing experience.",

    "Came in for a {product} — worst smoothie I've had. {product_adj} and the {issue}. "
    "Won't be coming back to this {state} store.",

    "The {product} was {product_adj}. For the price I expect much better. "
    "Staff seemed {staff_adj} and the {issue} made the whole visit unpleasant.",

    "Really bad experience today. My {product} ({visit_type}) was {product_adj}. "
    "There was a {issue} and nobody seemed to care.",

    "Ordered the {product} — {product_adj}. The {issue} is a recurring problem at this location. "
    "Staff was {staff_adj}. Management needs to address this.",

    "Not worth the money. {product} tasted {product_adj}. Long wait, {staff_adj} staff, "
    "and the {issue} was the last straw.",

    "I come here regularly but today was bad. {product} was {product_adj} and "
    "there was a clear {issue}. The {state} location needs improvement.",

    "Terrible visit. Waited 15 minutes for my {product} only to find it {product_adj}. "
    "The {issue} and {staff_adj} staff ruined my morning.",
]

NEUTRAL_TEMPLATES = [
    "The {product} was okay, nothing special. Service was average. "
    "Probably won't make a special trip but might stop by if I'm nearby.",

    "Decent {product} but the {issue} was a minor annoyance. Staff was fine. "
    "About what you'd expect for the price.",

    "Average experience. {product} tasted fine but felt a bit {product_adj}. "
    "The {state} location is convenient at least.",

    "Not bad, not great. {product} was acceptable. The {issue} could be improved. "
    "Would return but it's not my first choice.",

    "Mixed feelings. Love the {product} concept but today's execution was just okay. "
    "Staff was {staff_adj} but not unfriendly.",

    "It's fine. Nothing special about the {product}. Could do with some improvements "
    "around {issue} and consistency.",
]

POSITIVE_TEMPLATES = [
    "Loved my {product} today! The {state} location always delivers. "
    "Staff was friendly and my order was ready quickly.",

    "Perfect {product} as always. Great value and the staff at the {state} store "
    "are always so welcoming.",

    "Fantastic {visit_type} experience. My {product} was delicious and freshly made. "
    "Will definitely be back!",

    "Five stars! The {product} was amazing — exactly what I needed. "
    "The team at {state} are consistently excellent.",

    "Best smoothie in {state}! My {product} was spot on. Fast service and a clean store. "
    "Highly recommend.",

    "Always a great stop. {product} was delicious and the staff made me feel welcome. "
    "The {state} location never disappoints.",
]

BAD_PRODUCT_ADJ   = ["watery", "too sweet", "bitter", "lukewarm", "oddly flavoured",
                     "not fresh", "poorly blended", "disappointing"]
BAD_STAFF_ADJ     = ["rude", "dismissive", "unhelpful", "inattentive", "unapologetic",
                     "disorganised", "slow"]
BAD_ISSUES        = ["dirty tables", "wrong order", "broken blender noise",
                     "long queue with no apology", "incorrect ingredients",
                     "missing supplement I paid for", "cold drink when I asked for it chilled",
                     "spilled drink not replaced", "out-of-stock items not mentioned upfront"]


# ── Product loader ─────────────────────────────────────────────────────────────
def load_products():
    products = []
    with open("data/products.csv", newline="") as f:
        for row in csv.DictReader(f):
            products.append({
                "product_id": row["ID"],
                "item":       row["Item"],
                "category":   row["Category"],
            })
    return products


# ── Event factory ──────────────────────────────────────────────────────────────
def make_feedback(products):
    product    = random.choice(products)
    state      = random.choice(US_STATES)
    visit_type = random.choice(VISIT_TYPES)

    # Weighted score distribution: 70% positive, 15% neutral, 15% bad
    band = random.choices(["bad", "neutral", "positive"], weights=[15, 15, 70])[0]

    if band == "bad":
        overall_score = random.randint(1, 4)
        pq  = max(1, overall_score + random.randint(-1, 1))
        ss  = max(1, overall_score + random.randint(-2, 1))
        sf  = max(1, overall_score + random.randint(-2, 1))
        cl  = max(1, overall_score + random.randint(-1, 2))
        vfm = max(1, overall_score + random.randint(-1, 1))
        template = random.choice(BAD_TEMPLATES).format(
            product=product["item"],
            state=state,
            visit_type=visit_type,
            product_adj=random.choice(BAD_PRODUCT_ADJ),
            staff_adj=random.choice(BAD_STAFF_ADJ),
            issue=random.choice(BAD_ISSUES),
        )
    elif band == "neutral":
        overall_score = random.randint(5, 6)
        pq  = min(10, overall_score + random.randint(-1, 2))
        ss  = min(10, overall_score + random.randint(-1, 2))
        sf  = min(10, overall_score + random.randint(-1, 2))
        cl  = min(10, overall_score + random.randint(0, 2))
        vfm = min(10, overall_score + random.randint(-1, 1))
        template = random.choice(NEUTRAL_TEMPLATES).format(
            product=product["item"],
            state=state,
            visit_type=visit_type,
            product_adj=random.choice(["a bit thin", "slightly off", "not as good as usual"]),
            staff_adj=random.choice(["fine", "okay", "average"]),
            issue=random.choice(BAD_ISSUES[:4]),
        )
    else:
        overall_score = random.randint(7, 10)
        pq  = min(10, overall_score + random.randint(0, 1))
        ss  = min(10, overall_score + random.randint(-1, 1))
        sf  = min(10, overall_score + random.randint(0, 1))
        cl  = min(10, overall_score + random.randint(0, 1))
        vfm = min(10, overall_score + random.randint(-1, 1))
        template = random.choice(POSITIVE_TEMPLATES).format(
            product=product["item"],
            state=state,
            visit_type=visit_type,
        )

    return {
        "review_id":          "REV-" + uuid.uuid4().hex[:10].upper(),
        "transaction_id":     "TX-"  + uuid.uuid4().hex[:12].upper(),
        "customer_id":        "CUST-" + uuid.uuid4().hex[:8].upper(),
        "store_state":        state,
        "product_name":       product["item"],
        "product_category":   product["category"],
        "overall_score":      overall_score,
        "product_quality":    pq,
        "service_speed":      ss,
        "staff_friendliness": sf,
        "cleanliness":        cl,
        "value_for_money":    vfm,
        "review_text":        template,
        "visit_type":         visit_type,
        "would_return":       overall_score >= 7,
        "event_time":         datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
    }


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    products = load_products()
    print(f"feedback_generator started: {len(products)} products loaded")
    print(f"  Kafka: {BOOTSTRAP}  →  topic: {TOPIC_FEEDBACK}")
    print(f"  Score distribution: 70% positive (7-10), 15% neutral (5-6), 15% bad (1-4)")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    count = 0
    while True:
        event = make_feedback(products)
        producer.send(TOPIC_FEEDBACK, value=event)
        band = "BAD    " if event["overall_score"] < 5 else \
               "NEUTRAL" if event["overall_score"] < 7 else "GOOD   "
        print(f"[{band}] score={event['overall_score']} "
              f"product={event['product_name']} ({event['product_category']}) "
              f"state={event['store_state']} review_id={event['review_id']}")
        count += 1
        # Rate from [FEEDBACK] in configuration.ini — far below the purchase rate
        time.sleep(random.uniform(MIN_FREQ, MAX_FREQ))


if __name__ == "__main__":
    main()
