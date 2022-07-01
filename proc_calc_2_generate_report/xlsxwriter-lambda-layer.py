"""
......This scrpit is for creating xlsWriter lambda layer......
"""
import os

os.system(
    """
    echo ".............All good! Ready to go!............."
    export ZIP_FILE_NAME="layer-python-xlswriter.zip" 
    cd ~/Desktop
    mkdir -p python/lib/python3.7/site-packages
    pip3 install XlsxWriter --target ~/Desktop/python/lib/python3.7/site-packages
    zip -r xlswriter.zip ./python 
    echo ".............Check the file:${ZIP_FILE_NAME} on your Desktop............."
    """
)
