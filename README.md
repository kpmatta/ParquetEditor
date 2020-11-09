## Parquet Editor

This is to LOAD and EDIT **SMALL** parquet files.

### Open Parquet
* Open Parquet command shows a Folder dialog to select the parquet file **folder**.
* Selected parquet will be converted to Json format in the Editor for updating the Data.
* After opening the parquet file, parquet schema set to input schema path as inferred.

### Open Json
* Open Json command shows a File dialog to select the desired single json file. 
* Note that before opening the Json file, input schema file to be loaded, to format the Json to the appropriate format.

### Save
* Save command to save/overwrite the changes to the current file.
* It can save parquet and Json files.
* While Saving the file, Data will be validated against the Schema. Exception will be raised for and errors and restores the current file data.

### Save As Parquet
* Save as Parquet shows a Folder dialog to select the new parquet to save as different file.

### Save As Json
* Save as Json shows a File dialog to select the new Json file path to save as different file. 

### Generate Schema
* To create a schema file from the selected parquet file. 
* Parquet should be opened first to generate the schema file.

### Close
* Clears the editor.

### Exit
* Exists the application.

## Start Application
java -jar build/libs/ParquetEditor-1.0-SNAPSHOT.jar