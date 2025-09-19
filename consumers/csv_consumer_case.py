"""
csv_consumer_case.py

Consume json messages from a Kafka topic and visualize temperature data in real-time.

Example Kafka message format:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 225.0}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
import time
from datetime import datetime

# Use a deque ("deck") - a double-ended queue data structure
# A deque is a good way to monitor a certain number of "most recent" messages
# A deque is a great data structure for time windows (e.g. the last 5 messages)
from collections import deque

# Import external packages
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError, KafkaTimeoutError

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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
    topic = os.getenv("SMOKER_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_kafka_broker_address() -> str:
    """Fetch Kafka broker address from environment or use default."""
    broker = os.getenv("KAFKA_BROKER_ADDRESS", "localhost:9092")
    logger.info(f"Kafka broker address: {broker}")
    return broker


def get_stall_threshold() -> float:
    """Fetch message interval from environment or use default."""
    temp_variation = float(os.getenv("SMOKER_STALL_THRESHOLD_F", 0.2))
    return temp_variation


def get_rolling_window_size() -> int:
    """Fetch rolling window size from environment or use default."""
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size


def get_processing_delay() -> float:
    """Fetch processing delay from environment or use default."""
    delay = float(os.getenv("MESSAGE_PROCESSING_DELAY", "2.0"))
    logger.info(f"Message processing delay: {delay} seconds")
    return delay


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
# Set up data structures (empty lists)
#####################################

timestamps = []  # To store timestamps for the x-axis
temperatures = []  # To store temperature readings for the y-axis

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots(figsize=(12, 8))

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()


#####################################
# Define a function to detect a stall
#####################################


def detect_stall(rolling_window_deque: deque, window_size: int) -> bool:
    """
    Detect a temperature stall based on the rolling window.

    Args:
        rolling_window_deque (deque): Rolling window of temperature readings.

    Returns:
        bool: True if a stall is detected, False otherwise.
    """
    if len(rolling_window_deque) < window_size:
        # We don't have a full deque yet
        # Keep reading until the deque is full
        logger.debug(
            f"Rolling window current size: {len(rolling_window_deque)}. Waiting for {window_size}."
        )
        return False

    # Once the deque is full we can calculate the temperature range
    # Use Python's built-in min() and max() functions
    # If the range is less than or equal to the threshold, we have a stall
    # And our food is ready :)
    temp_range = max(rolling_window_deque) - min(rolling_window_deque)
    is_stalled: bool = temp_range <= get_stall_threshold()
    if is_stalled:
        logger.debug(f"Temperature range: {temp_range}°F. Stalled: {is_stalled}")
    return is_stalled


#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart(rolling_window, window_size):
    """
    Update temperature vs. time chart.
    Args:
        rolling_window (deque): Rolling window of temperature readings.
        window_size (int): Size of the rolling window.
    """
    try:
        # Clear the previous chart
        ax.clear()  

        if not timestamps or not temperatures:
            logger.debug("No data to plot yet")
            return

        # Convert timestamp strings to datetime objects for better plotting
        datetime_timestamps = []
        for ts in timestamps:
            try:
                if isinstance(ts, str):
                    # Handle ISO format timestamps
                    dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    datetime_timestamps.append(dt)
                else:
                    # If it's already a datetime or number, use as is
                    datetime_timestamps.append(ts)
            except (ValueError, AttributeError):
                # If timestamp parsing fails, use string as is
                datetime_timestamps.append(ts)

        # Create a line chart using the plot() method
        # Use the timestamps for the x-axis and temperatures for the y-axis
        # Use the label parameter to add a legend entry
        # Use the color parameter to set the line color
        ax.plot(datetime_timestamps, temperatures, label="Temperature", color="blue", linewidth=2)

        # Use the built-in axes methods to set the labels and title
        ax.set_xlabel("Time")
        ax.set_ylabel("Temperature (°F)")
        ax.set_title("Smart Smoker: Temperature vs. Time - Elom Gbogbo")

        # Highlight stall points if conditions are met such that
        #    The rolling window is full and a stall is detected
        if len(rolling_window) >= window_size and detect_stall(rolling_window, window_size):
            # Mark the stall point on the chart

            # An index of -1 gets the last element in a list
            stall_time = datetime_timestamps[-1]
            stall_temp = temperatures[-1]

            # Use the scatter() method to plot a point
            # Pass in the x value as a list, the y value as a list (using [])
            # and set the marker color and label
            # zorder is used to ensure the point is plotted on TOP of the line chart
            # zorder of 5 is higher than the default zorder of 2
            ax.scatter(
                [stall_time], [stall_temp], color="red", s=100, label="Stall Detected", zorder=5
            )

            # Use the annotate() method to add a text label
            # To learn more, look up the matplotlib axes.annotate documentation
            # https://matplotlib.org/stable/api/_as_gen/matplotlib.axes.Axes.annotate.html
            # textcoords="offset points" means the label is placed relative to the point
            # xytext=(10, -10) means the label is placed 10 points to the right and 10 points down from the point 
            # Typically, the first number is x (horizontal) and the second is y (vertical)
            # x: Positive moves to the right, negative to the left
            # y: Positive moves up, negative moves down
            # ha stands for horizontal alignment
            # We set color to red, a common convention for warnings
            ax.annotate(
                "Stall Detected",
                (stall_time, stall_temp),
                textcoords="offset points",
                xytext=(10, -10),
                ha="center",
                color="red",
                fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow", alpha=0.7)
            )

        # Regardless of whether a stall is detected, we want to show the legend
        # Use the legend() method to display the legend
        ax.legend()

        # Format x-axis for better date/time display
        if datetime_timestamps and isinstance(datetime_timestamps[0], datetime):
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            ax.xaxis.set_major_locator(mdates.SecondLocator(interval=30))
            fig.autofmt_xdate()

        # Use the tight_layout() method to automatically adjust the padding
        plt.tight_layout()

        # Draw the chart
        plt.draw()

        # Pause longer to slow down chart updates
        plt.pause(0.5)  # Increased from 0.01 to 0.5 seconds
        
    except Exception as e:
        logger.warning(f"Error updating chart: {e}")


#####################################
# Function to process a single message
# #####################################


def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a JSON-transferred CSV message and check for stalls.

    Args:
        message (str): JSON message received from Kafka.
        rolling_window (deque): Rolling window of temperature readings.
        window_size (int): Size of the rolling window.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        data: dict = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        # Ensure the required fields are present
        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        # Convert temperature to float if it's not already
        try:
            temperature = float(temperature)
        except (ValueError, TypeError):
            logger.error(f"Invalid temperature value: {temperature}")
            return

        # Append the temperature reading to the rolling window
        rolling_window.append(temperature)

        # Append the timestamp and temperature to the chart data
        timestamps.append(timestamp)
        temperatures.append(temperature)

        # Update chart after processing this message
        update_chart(rolling_window=rolling_window, window_size=window_size)

        # Check for a stall
        if detect_stall(rolling_window, window_size):
            logger.info(
                f"STALL DETECTED at {timestamp}: Temp stable at {temperature}°F over last {window_size} readings."
            )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Enhanced polling function
#####################################


def poll_messages_robust(consumer: KafkaConsumer, topic: str, rolling_window: deque, window_size: int) -> None:
    """
    Poll messages from Kafka with robust error handling.
    
    Args:
        consumer (KafkaConsumer): The Kafka consumer instance
        topic (str): The topic name for logging
        rolling_window (deque): Rolling window for temperature readings
        window_size (int): Size of the rolling window
    """
    logger.info(f"Starting robust message polling from topic '{topic}'...")
    
    consecutive_errors = 0
    max_consecutive_errors = 5
    processing_delay = get_processing_delay()
    
    while True:
        try:
            # Poll for messages with longer timeout and fewer records for slower processing
            message_batch = consumer.poll(timeout_ms=3000, max_records=3)
            
            if message_batch:
                consecutive_errors = 0  # Reset error count on success
                
                for topic_partition, messages in message_batch.items():
                    logger.debug(f"Received {len(messages)} messages from {topic_partition}")
                    for message in messages:
                        message_str = message.value
                        logger.debug(f"Processing message at offset {message.offset}")
                        process_message(message_str, rolling_window, window_size)
                        
                        # Add delay after processing each message
                        time.sleep(processing_delay)
            else:
                # No messages received, this is normal
                logger.debug("No messages received in this poll cycle")
                time.sleep(2.0)  # Wait 2 seconds before next poll
                
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

    # Clear previous run's data
    timestamps.clear()
    temperatures.clear()

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(f"Rolling window size: {window_size}")
    rolling_window = deque(maxlen=window_size)

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
        poll_messages_robust(consumer, topic, rolling_window, window_size)
        
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

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
    plt.ioff()  # Turn off interactive mode after completion
    plt.show()