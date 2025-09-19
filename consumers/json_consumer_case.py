"""
json_consumer_case.py

Consume json messages from a Kafka topic and visualize author counts in real-time.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
import time
from collections import defaultdict  # data structure for counting author occurrences

# Import external packages
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_kafka_broker_address() -> str:
    """Fetch Kafka broker address from environment or use default."""
    broker = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
    logger.info(f"Kafka broker address: {broker}")
    return broker


#####################################
# Alternative Consumer Creation Function
#####################################


def create_robust_kafka_consumer(topic: str, group_id: str) -> KafkaConsumer:
    """
    Create a robust Kafka consumer with fallback configurations.
    
    Args:
        topic (str): The Kafka topic to consume from
        group_id (str): The consumer group ID
        
    Returns:
        KafkaConsumer: Configured Kafka consumer instance
    """
    broker = get_kafka_broker_address()
    
    # Try different configurations
    configs_to_try = [
        # Configuration 1: Minimal and robust
        {
            'bootstrap_servers': [broker],
            'group_id': group_id,
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'value_deserializer': lambda x: x.decode('utf-8') if x else None,
            'consumer_timeout_ms': 1000,
            'request_timeout_ms': 30000,
            'session_timeout_ms': 10000,
            'heartbeat_interval_ms': 3000,
        },
        # Configuration 2: With explicit API version
        {
            'bootstrap_servers': [broker],
            'group_id': group_id,
            'auto_offset_reset': 'latest',
            'enable_auto_commit': True,
            'value_deserializer': lambda x: x.decode('utf-8') if x else None,
            'api_version': (0, 10, 1),
            'consumer_timeout_ms': 1000,
        },
        # Configuration 3: Very basic
        {
            'bootstrap_servers': [broker],
            'group_id': group_id,
            'auto_offset_reset': 'latest',
            'value_deserializer': lambda x: x.decode('utf-8') if x else None,
        }
    ]
    
    for i, config in enumerate(configs_to_try, 1):
        try:
            logger.info(f"Trying consumer configuration {i}...")
            consumer = KafkaConsumer(**config)
            consumer.subscribe([topic])
            logger.info(f"Consumer created successfully with configuration {i}")
            return consumer
        except Exception as e:
            logger.warning(f"Configuration {i} failed: {e}")
            if i < len(configs_to_try):
                logger.info(f"Trying next configuration...")
            continue
    
    raise Exception("All consumer configurations failed")


#####################################
# Set up data structures
#####################################

# Initialize a dictionary to store author counts
author_counts = defaultdict(int)

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart():
    """Update the live chart with the latest author counts."""
    try:
        # Clear the previous chart
        ax.clear()

        # Get the authors and counts from the dictionary
        authors_list = list(author_counts.keys())
        counts_list = list(author_counts.values())

        # Create a bar chart using the bar() method.
        # Pass in the x list, the y list, and the color
        ax.bar(authors_list, counts_list, color="skyblue")

        # Use the built-in axes methods to set the labels and title
        ax.set_xlabel("Authors")
        ax.set_ylabel("Message Counts")
        ax.set_title("Real-Time Author Message Counts - Elom Gbogbo")

        # Use the set_xticklabels() method to rotate the x-axis labels
        # Pass in the x list, specify the rotation angle is 45 degrees,
        # and align them to the right
        # ha stands for horizontal alignment
        if authors_list:  # Only set labels if there are authors
            ax.set_xticklabels(authors_list, rotation=45, ha="right")

        # Use the tight_layout() method to automatically adjust the padding
        plt.tight_layout()

        # Draw the chart
        plt.draw()

        # Pause briefly to allow some time for the chart to render
        plt.pause(0.01)
    except Exception as e:
        logger.warning(f"Error updating chart: {e}")


#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka and update the chart.

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
            # Extract the 'author' field from the Python dictionary
            author = message_dict.get("author", "unknown")
            logger.info(f"Message received from author: {author}")

            # Increment the count for the author
            author_counts[author] += 1

            # Log the updated counts
            logger.info(f"Updated author counts: {dict(author_counts)}")

            # Update the chart
            update_chart()

            # Log the updated chart
            logger.info(f"Chart updated successfully for message: {message}")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Enhanced polling function
#####################################


def poll_messages_robust(consumer: KafkaConsumer, topic: str) -> None:
    """
    Poll messages from Kafka with robust error handling.
    
    Args:
        consumer (KafkaConsumer): The Kafka consumer instance
        topic (str): The topic name for logging
    """
    logger.info(f"Starting robust message polling from topic '{topic}'...")
    
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    while True:
        try:
            # Poll for messages with timeout
            message_batch = consumer.poll(timeout_ms=1000, max_records=10)
            
            if message_batch:
                consecutive_errors = 0  # Reset error count on success
                
                for topic_partition, messages in message_batch.items():
                    logger.debug(f"Received {len(messages)} messages from {topic_partition}")
                    for message in messages:
                        message_str = message.value
                        logger.debug(f"Processing message at offset {message.offset}")
                        process_message(message_str)
                        time.sleep(1.0)  # Add 1 second delay after each message
            else:
                # No messages received, this is normal
                logger.debug("No messages received in this poll cycle")
                time.sleep(2.0)  # Increased delay - poll every 2 seconds
                
        except KafkaTimeoutError:
            logger.debug("Poll timeout - this is normal, continuing...")
            continue
            
        except KafkaError as e:
            consecutive_errors += 1
            logger.error(f"Kafka error during polling (#{consecutive_errors}): {e}")
            
            if consecutive_errors >= max_consecutive_errors:
                logger.error(f"Too many consecutive errors ({consecutive_errors}). Stopping consumer.")
                break
                
            time.sleep(1)  # Wait before retrying
            
        except KeyboardInterrupt:
            logger.warning("Consumer interrupted by user (Ctrl+C)")
            break
            
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"Unexpected error during polling (#{consecutive_errors}): {e}")
            
            if consecutive_errors >= max_consecutive_errors:
                logger.error(f"Too many consecutive errors ({consecutive_errors}). Stopping consumer.")
                break
                
            time.sleep(1)  # Wait before retrying


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer with enhanced error handling.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    consumer = None
    try:
        # Try the original consumer creation first
        try:
            logger.info("Attempting to create consumer using utils_consumer...")
            consumer = create_kafka_consumer(topic, group_id)
        except Exception as e:
            logger.warning(f"Original consumer creation failed: {e}")
            logger.info("Trying robust consumer creation...")
            consumer = create_robust_kafka_consumer(topic, group_id)

        # Start polling with robust error handling
        poll_messages_robust(consumer, topic)
        
    except Exception as e:
        logger.error(f"Fatal error in consumer: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if consumer:
            try:
                consumer.close()
                logger.info(f"Kafka consumer for topic '{topic}' closed.")
            except Exception as e:
                logger.warning(f"Error closing consumer: {e}")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    # Call the main function to start the consumer
    main()

    # Turn off interactive mode after completion
    plt.ioff()  

    # Display the final chart
    plt.show()