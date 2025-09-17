# Canvas → Notion Sync

Automates pulling due items from Canvas and updating a Notion database. Runs multiple times per day without duplicates. Efficient by default.

## What it does

- Fetches assignments and quizzes for your chosen Canvas courses
- Detects quizzes via `assignment.quiz_id` from the Assignments API
- Writes and updates these Notion properties:
  - **Task** (Title)
  - **Class** (Select)
  - **Type** (Select: Assignment, Quiz, Exam)
  - **Due** (Date)
  - **Time** (Text from Due)
  - **Status** (Status or Select: To do, In Progress, Complete, DNF)
  - **Key** (Text hidden, used for safe upserts)
- Updates rows if status or due dates change
- Uses a small delta check between runs to minimize API calls

## Requirements

- Python 3.10+
- A Notion integration with edit access to your database
- A Canvas access token
- Your Canvas base URL, example `https://canvas.asu.edu`

## Notion setup

1. Create a database in Notion and share it with your integration with Edit access.
2. Ensure the database has these properties with exact names:
   - **Task** type Title
   - **Class** type Select
   - **Type** type Select
   - **Due** type Date
   - **Time** type Text
   - **Status** type Status preferred. Select also works.
   - **Key** type Text. Hide this column in views.

The script can create missing properties, but matching names and types avoids surprises.
