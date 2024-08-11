import os
import datetime
import logging
import importlib
import subprocess
import sys
import pyodbc
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import schedule
import time

# Required libraries and their respective pip package names
REQUIRED_LIBRARIES = {
    'pyodbc': 'pyodbc',
    'smtplib': None,
    'email': None,
    'schedule': 'schedule'
}

# Configure logging
logging.basicConfig(filename='database_backup.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def install_package(package_name):
    """Install the package using pip."""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        logging.info(f"{package_name} installed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to install {package_name}: {e}")
        print(f"Failed to install {package_name}. Please install it manually.")

def check_libraries():
    """Check if required libraries are installed, and install them if they are missing."""
    for package, module_name in REQUIRED_LIBRARIES.items():
        try:
            if module_name:
                importlib.import_module(module_name)
            logging.info(f"{package} is installed.")
        except ImportError:
            logging.error(f"{package} is not installed. Attempting to install...")
            print(f"{package} is not installed. Attempting to install...")
            if module_name:
                install_package(module_name)
            else:
                print(f"{package} is a standard library and should be available in your Python installation.")

# Check and install required libraries
check_libraries()

def connect_to_sql_server(server, database, username, password):
    """Connect to SQL Server and return the connection."""
    try:
        connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=no;TrustServerCertificate=yes;'
        connection = pyodbc.connect(connection_string)
        logging.info("Connected to SQL Server successfully.")
        return connection
    except Exception as e:
        logging.error(f"Failed to connect to SQL Server: {e}")
        raise

def create_backup_directory(base_dir):
    """Create directories based on year, month, and day."""
    today = datetime.date.today()
    year_dir = os.path.join(base_dir, str(today.year))
    month_dir = os.path.join(year_dir, str(today.month).zfill(2))  # Zero pad the month
    day_dir = os.path.join(month_dir, str(today.day).zfill(2))     # Zero pad the day

    for directory in [year_dir, month_dir, day_dir]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logging.info(f"Created directory: {directory}")
    
    return day_dir

def backup_database(connection, database_name, backup_dir):
    """Back up the specified database and return the backup file path."""
    try:
        cursor = connection.cursor()
        
        # Generate the backup file name
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f'{database_name}_backup_{timestamp}.bak'
        backup_path = os.path.join(backup_dir, backup_filename)
        
        # Construct the SQL command to back up the database
        backup_sql = f"BACKUP DATABASE [{database_name}] TO DISK = N'{backup_path}' WITH NOFORMAT, NOINIT, NAME = N'{database_name}-Full Database Backup', SKIP, NOREWIND, NOUNLOAD, STATS = 10"
        
        # Execute the backup command
        cursor.execute(backup_sql)
        cursor.commit()
        
        logging.info(f"Backup of database {database_name} completed successfully. Saved to {backup_path}")
        
        return backup_path
    except Exception as e:
        logging.error(f"Failed to back up the database: {e}")
        raise

def send_email_notification(subject, body, from_email, to_email, smtp_server, smtp_port, smtp_user, smtp_password):
    """Send an email notification."""
    try:
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(from_email, to_email, msg.as_string())
            logging.info("Email notification sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send email notification: {e}")

def perform_backup():
    """Perform the backup operation."""
    try:
        # Configuration parameters
        server = "YOUR_SERVER"
        database = "YOUR_DATABASE"
        username = "YOUR_USERNAME"
        password = "YOUR_PASSWORD"
        backup_dir = "/path/to/backup"
        notify_email = "recipient@example.com"
        smtp_server = "smtp.example.com"
        smtp_port = 587
        smtp_user = "smtp_user"
        smtp_password = "smtp_password"

        # Create backup directory based on the current date
        backup_dir = create_backup_directory(backup_dir)

        # Connect to the database
        connection = connect_to_sql_server(server, database, username, password)

        # Backup the database
        backup_path = backup_database(connection, database, backup_dir)

        # Close the database connection
        connection.close()
        logging.info("Database connection closed.")

        # Send success email notification
        if notify_email:
            subject = f"Backup Success: {database}"
            body = f"Backup of database {database} completed successfully. Backup file: {backup_path}"
            send_email_notification(subject, body, smtp_user, notify_email, smtp_server, smtp_port, smtp_user, smtp_password)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        if notify_email:
            subject = f"Backup Failed: {database}"
            body = f"An error occurred during the backup of database {database}. Error: {e}"
            send_email_notification(subject, body, smtp_user, notify_email, smtp_server, smtp_port, smtp_user, smtp_password)

def main():
    """Main function to set up scheduled backups."""
    # Schedule the backup every Friday at 8:00 AM
    schedule.every().friday.at("08:00").do(perform_backup)

    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Wait one minute

if __name__ == "__main__":
    main()
