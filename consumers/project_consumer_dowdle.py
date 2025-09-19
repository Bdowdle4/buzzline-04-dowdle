"""
project_consumer_dowdle.py

Consumer uses a rolling window of the last 20 messages per author.

"""

#####################################
# Import Modules
#####################################

import json
import matplotlib.pyplot as plt
from collections import defaultdict, deque
import pathlib

#####################################
# Set up data structures
#####################################

# Match the producer's setup
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("project_live.json")

# Rolling window size
WINDOW_SIZE = 20

# Dictionary of deques for rolling windows per author
author_sentiments = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))

#####################################
# Set up live visuals
#####################################

plt.ion()
fig, ax = plt.subplots()

#####################################
# Process Message Function
#####################################

def process_message(message):
    """Process each JSON message (as dict)."""
    author = message.get("author")
    sentiment = message.get("sentiment")

    if author and sentiment is not None:
        # Add sentiment to rolling window
        author_sentiments[author].append(sentiment)

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################

def update_chart():
    """Update live chart with rolling averages."""
    ax.clear()

    authors = list(author_sentiments.keys())
    averages = [
        sum(values) / len(values) for values in author_sentiments.values()
    ]

    ax.bar(authors, averages, color="green")
    ax.set_ylim(0, 1)
    ax.set_ylabel("Average Sentiment (Rolling)")
    ax.set_title(f"Rolling Average Sentiment (last {WINDOW_SIZE} messages)")

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.pause(0.1)

#####################################
# Main Function
#####################################

def main():
    """Read from project_producer_case output file and update chart."""
    with DATA_FILE.open("r") as f:
        for line in f:
            try:
                message = json.loads(line.strip())
                process_message(message)
                update_chart()
            except json.JSONDecodeError:
                continue

    print("Consumer finished reading messages.")


if __name__ == "__main__":
    main()
