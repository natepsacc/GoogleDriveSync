# Google Drive Sync  

## Overview  
This script provides an automated way to sync files between a Google Drive folder and a local directory, ensuring that files are downloaded, moved to an output directory, and logs are uploaded back to Drive. 

- **Google Drive Sync**: Downloads files from a specified Drive folder.  
- **Local File Management**: Moves files from the local sync directory to an output directory.  
- **Logging**: Maintains a log file (`drive_sync.log`) and uploads it to Google Drive.  
- **Continuous Sync Mode**: Runs the sync process at regular intervals.  

## Requirements  
- req.txt

## Setup  
1. **Create a Google Cloud Project & Enable Drive API**  
   - Generate a **Service Account Key** (JSON format).  

2. **Set up `.env` file**  
   Create a `.env` file in the project directory and add:  
   ```ini
   CREDENTIALS_PATH=path/to/your-service-account.json
   DRIVE_FOLDER_ID=your_drive_folder_id
   LOCAL_FOLDER=path/to/local/folder
   OUTPUT_FOLDER=path/to/output/folder
   ```  

