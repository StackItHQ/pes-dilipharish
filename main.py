import os
import mysql.connector
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
import time

# MySQL connection parameters
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "93420D@l"
MYSQL_DB = "superjoin"

# Google Sheets API scopes
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Google Sheets ID and range
SPREADSHEET_ID = "1JtlSEP1-QXj0Kg4uxBXuiXMdYD0z_O6V7Q74ifZnNhQ"
RANGE_NAME = "Sheet1!A1:B"  # Assuming columns are Name and Email


def get_mysql_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DB
    )


def fetch_all_records():
    """Fetch all records from MySQL."""
    conn = get_mysql_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, email FROM users")
    records = cursor.fetchall()
    conn.close()
    return records


def fetch_existing_records():
    """Fetch existing records from MySQL."""
    records = fetch_all_records()
    existing_records = {
        (row[2]): (row[0], row[1]) for row in records  # Map email to (id, name)
    }
    return existing_records


def sync_google_sheet_to_mysql(values):
    conn = get_mysql_connection()
    cursor = conn.cursor()
    existing_records = fetch_existing_records()
    sheet_records = {
        (row[1]): row[0] for row in values if len(row) == 2
    }  # email to name

    new_entries = []
    updates = []
    deletions = []

    # Check for new entries and updates
    for email, name in sheet_records.items():
        if email not in existing_records:
            print(f"Inserting record: ({name}, {email})")
            cursor.execute(
                "INSERT INTO users (name, email) VALUES (%s, %s)", (name, email)
            )
            new_entries.append((name, email))
        else:
            # Record exists, check if it needs to be updated
            id, existing_name = existing_records[email]
            if existing_name != name:
                print(f"Updating record: ({name}, {email})")
                cursor.execute(
                    "UPDATE users SET name = %s WHERE email = %s", (name, email)
                )
                updates.append((name, email))

    # Check for deletions
    for email, (id, _) in existing_records.items():
        if email not in sheet_records:
            print(f"Deleting record: ({id}, {email})")
            cursor.execute("DELETE FROM users WHERE id = %s", (id,))
            deletions.append((email,))

    conn.commit()
    cursor.close()
    conn.close()

    return new_entries, updates, deletions


def main():
    """Sync data from Google Sheets to MySQL."""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Error refreshing credentials: {e}")
                return
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", SCOPES
                )
                creds = flow.run_local_server(port=0)
                with open("token.json", "w") as token:
                    token.write(creds.to_json())
            except Exception as e:
                print(f"Error during OAuth2 flow: {e}")
                return

    # Call the Sheets API
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    while True:
        # Sync data from Google Sheets to MySQL
        try:
            result = (
                sheet.values()
                .get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME)
                .execute()
            )
            values = result.get("values", [])

            if not values:
                print("No data found in Google Sheets.")
            else:
                print("Syncing Google Sheets data to MySQL...")
                new_entries, updates, deletions = sync_google_sheet_to_mysql(values)
                if new_entries:
                    print("New entries added:")
                    for entry in new_entries:
                        print(entry)
                if updates:
                    print("Records updated:")
                    for entry in updates:
                        print(entry)
                if deletions:
                    print("Records deleted:")
                    for entry in deletions:
                        print(entry)
                if not new_entries and not updates and not deletions:
                    print("No changes detected.")

        except Exception as e:
            print(f"Error reading Google Sheets data: {e}")

        time.sleep(6)  # Wait for 60 seconds before checking again


if __name__ == "__main__":
    main()
