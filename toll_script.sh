#!/bin/bash


# prompts the user to confirm before running the data
echo "Ready to run? [1/0]"
read run

# if the user select 1 then run the etl_toll_data script
if [ "$run" -eq 1 ]
then 
    echo "Running"
    python etl_toll_data.py
    if [ $? -eq 0 ]
    then
        echo "Script executed successfully."
    else
        echo "There was an error running the script."
    fi
else
    echo "Please come back when you are ready"
fi