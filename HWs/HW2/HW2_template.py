from lxml import etree
import time
import sys

### helper functions





# Function to Emulate dfs ls command
def emulate_ls(fsimage_path,object_path):
    # INPUT : fsimage_path in your local system ; path of object (directory or file) you are trying to list using ls command
    # RETURN : Space separated string which is expected output of ls command as provided in the handout. 
    # EXPECTED RETURN : (sample output) "drwxr-xr-x - ubuntu supergroup 0 2024-09-23 16:03 /user/john" [OR] ""
    return



# Main execution logic
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python <yourname>_hw2.py <fsimage> -ls <object>")
        sys.exit(1)
    
    fsimage_path = sys.argv[1]
    object_path = sys.argv[3]

    print(emulate_ls(fsimage_path, object_path))