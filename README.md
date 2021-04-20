# z_bhavcopy_parser

Requirements:
Write a standalone Python Django web app/server that:

- Downloads the equity bhavcopy zip from the above page every day at 18:00 IST for the current date.
- Extracts and parses the CSV file in it.
- Writes the records into Redis with appropriate data structures (Fields: code, name, open, high, low, close).
- Renders a simple VueJS frontend with a search box that allows the stored entries to be searched by name and renders a table of results and optionally downloads    the results as CSV. Make this page look nice!
- The search needs to be performed on the backend using Redis.
