"""
project_consumer_gbogbo.py

Read a JSON-formatted file as it is being written and visualize message categories distribution.

Example JSON message:
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import json
import os # for file operations
import sys # to exit early
import time
import pathlib
from collections import defaultdict  # data structure for counting category occurrences

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
import matplotlib.pyplot as plt
import numpy as np

# Import functions from local modules
from utils.utils_logger import logger


#####################################
# Set up Paths - read from the file the producer writes
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FILE = DATA_FOLDER.joinpath("buzz_live.json")

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Data folder: {DATA_FOLDER}")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Set up data structures for category tracking
#####################################

category_counts = defaultdict(int)  # Dictionary to count messages per category

#####################################
# Set up live visuals
#####################################

fig, ax = plt.subplots(figsize=(12, 7))
plt.ion()  # Turn on interactive mode for live updates

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################

def update_chart():
    """Update the live chart with the latest category distribution."""
    # Clear the previous chart
    ax.clear()

    if len(category_counts) > 0:
        # Get the categories and counts from the dictionary
        categories_list = list(category_counts.keys())
        counts_list = list(category_counts.values())

        # Create a color map for different categories
        colors = plt.cm.Set3(np.linspace(0, 1, len(categories_list)))

        # Create a bar chart using the bar() method
        bars = ax.bar(categories_list, counts_list, color=colors, edgecolor='black', linewidth=0.5)

        # Add value labels on top of each bar
        for bar, count in zip(bars, counts_list):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                   f'{count}', ha='center', va='bottom', fontweight='bold')

        # Set labels and title
        ax.set_xlabel("Message Categories", fontsize=12, fontweight='bold')
        ax.set_ylabel("Number of Messages", fontsize=12, fontweight='bold')
        
        # Calculate total messages and most popular category
        total_messages = sum(counts_list)
        most_popular = max(categories_list, key=lambda x: category_counts[x])
        most_popular_count = category_counts[most_popular]
        
        ax.set_title(f"Real-Time Message Categories Distribution - Elom Gbogbo\n"
                    f"Total Messages: {total_messages} | Most Popular: {most_popular} ({most_popular_count})",
                    fontsize=14, fontweight='bold')

        # Rotate x-axis labels for better readability
        ax.set_xticklabels(categories_list, rotation=45, ha="right")

        # Add grid for better readability
        ax.grid(True, alpha=0.3, axis='y')

        # Set y-axis to start from 0 and add some padding at the top
        ax.set_ylim(0, max(counts_list) * 1.1)

    else:
        # Show empty chart with labels
        ax.set_xlabel("Message Categories", fontsize=12, fontweight='bold')
        ax.set_ylabel("Number of Messages", fontsize=12, fontweight='bold')
        ax.set_title("Real-Time Message Categories Distribution - Elom Gbogbo\nWaiting for messages...",
                    fontsize=14, fontweight='bold')
        ax.grid(True, alpha=0.3, axis='y')

    # Use tight_layout() to automatically adjust padding
    plt.tight_layout()

    # Draw the chart
    plt.draw()

    # Pause briefly to allow time for the chart to render
    plt.pause(0.01)


#####################################
# Process Message Function
#####################################

def process_message(message: str) -> None:
    """
    Process a single JSON message and update the category distribution chart.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)
       
        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        # Ensure it's a dictionary before accessing fields
        if isinstance(message_dict, dict):
            # Extract the 'category' field from the Python dictionary
            category = message_dict.get("category", "unknown")
            author = message_dict.get("author", "unknown")
            
            logger.info(f"Message from {author} in category: {category}")

            # Increment the count for this category
            category_counts[category] += 1

            # Log the updated counts
            total_messages = sum(category_counts.values())
            logger.info(f"Updated category counts: {dict(category_counts)}")
            logger.info(f"Total messages processed: {total_messages}")

            # Update the chart
            update_chart()

            # Log the chart update
            logger.info(f"Chart updated successfully for category: {category}")

        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main Function
#####################################

def main() -> None:
    """
    Main entry point for the consumer.
    - Monitors a file for new messages and updates a live category distribution chart.
    """

    logger.info("START category distribution consumer.")

    # Verify the file we're monitoring exists if not, exit early
    if not DATA_FILE.exists():
        logger.error(f"Data file {DATA_FILE} does not exist. Exiting.")
        sys.exit(1)

    try:
        # Try to open the file and read from it
        with open(DATA_FILE, "r") as file:

            # Move the cursor to the end of the file
            file.seek(0, os.SEEK_END)
            print("Category Distribution Consumer is ready and waiting for new JSON messages...")
            print("Tracking message categories in real-time...")

            # Initialize the chart with empty data
            update_chart()

            while True:
                # Read the next line from the file
                line = file.readline()

                # If we strip whitespace from the line and it's not empty
                if line.strip():  
                    # Process this new message
                    process_message(line)
                else:
                    # otherwise, wait a half second before checking again
                    logger.debug("No new messages. Waiting...")
                    delay_secs = 0.5 
                    time.sleep(delay_secs) 
                    continue 

    except KeyboardInterrupt:
        logger.info("Category distribution consumer interrupted by user.")
        print(f"\nFinal Category Statistics:")
        if category_counts:
            total = sum(category_counts.values())
            print(f"Total messages processed: {total}")
            print("Category breakdown:")
            for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total) * 100
                print(f"  {category}: {count} messages ({percentage:.1f}%)")
        else:
            print("No messages were processed.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        plt.ioff()
        plt.show()
        logger.info("Category distribution consumer closed.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()