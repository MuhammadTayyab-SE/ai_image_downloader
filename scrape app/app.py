import tkinter as tk
from tkinter import filedialog
from tkinter import ttk
from selenium import webdriver
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from concurrent.futures import ThreadPoolExecutor
from selenium.webdriver.common.by import By
from tkinter import messagebox
import logging
import gc
from PIL import Image
import pandas as pd
import os
import threading
import io
import re
from collections import defaultdict
class AppLogging():
    def __init__(self):
        self.logging = logging.getLogger(__name__)  # Create a logger for the class
        self.logging.setLevel(logging.DEBUG)

        # Optional: Add file handler or other handlers to the logger
        handler = logging.FileHandler('scraper.log', encoding='utf-8')
        handler.setLevel(logging.DEBUG)

        # Set a formatter
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # Add the handler to the logger
        self.logging.addHandler(handler)
    def get_logger(self):
        return self.logging

class ImageDownloaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Download Image")
        self.root.geometry("560x550")
        self.font = ('Helvetica', 10)

        # Output Folder
        self.output_folder_label = tk.Label(root, text="LOCATION IMAGE").grid(row=0, column=0, padx=20, pady=10, sticky='w')
        tk.Button(root, text="SELECT", command=self.select_output_folder).grid(row=0, column=3, padx=10, pady=10, sticky='w')

        # SELECT EXCELS SECONDARY
        self.image_excel_label = tk.Label(root, text="SELECT EXCELS SECONDARY").grid(row=1, column=0, padx=20, pady=10, sticky='w')
        tk.Button(root, text="SELECT", command=self.select_image_excels).grid(row=1, column=3, padx=10, pady=10, sticky='w')

        # Number of Threads
        self.threads_label = tk.Label(root, text="NUMBER OF THREADS").grid(row=2, column=0, padx=20, pady=10, sticky='w')
        self.threads_entry = tk.IntVar(value=8)  # Default value set to 8
        tk.Entry(root, textvariable=self.threads_entry,width=7, font=self.font).grid(row=2, column=3, padx=10, pady=10)
        
        # Select Excel for Excluded Images
        self.exclude_excel_label = tk.Label(root, text="EXCEL EXCLUDE IMAGE")
        self.exclude_excel_label.grid(row=3, column=0, padx=20, pady=10, sticky='w')
        self.exclude_button = tk.Button(root, text="SELECT", command=self.select_exclude_excel)
        self.exclude_button.grid(row=3, column=3, padx=10, pady=10, sticky='w')

        # Type Selection Buttons        
        self.type_selection = tk.StringVar(value="type1")
        tk.Radiobutton(root, text="TYPE 1", variable=self.type_selection, value="type1", command=self.show_exclude_button).grid(row=4, column=0, padx=90, pady=10, sticky='w')
        tk.Radiobutton(root, text="TYPE 2", variable=self.type_selection, value="type2", command=self.hide_exclude_button).grid(row=4, column=2, padx=10, pady=10, sticky='e')

        # Start Button
        tk.Button(root, text="START", command=self.start_thread, width=18, height=1).grid(row=5, column=1, padx=10, pady=20)

        # Process Output
        self.process_output_label = tk.Label(root, text="Process output:")
        self.process_output = tk.Text(root, height=12, width=65)
        self.process_output.grid(row=6, column=0, columnspan=5, pady=10, padx=0)
        self.progress_frame = tk.Frame(root)
        self.progress_frame.grid(row=7, column=0, columnspan=5)

        # Progress Bar
        self.progress = ttk.Progressbar(self.progress_frame, orient="horizontal", length=525, mode="determinate")
        self.progress.pack(side=tk.LEFT ,padx=10)
        self.counter_label = tk.Label(root, text="0/0", background='#D7D7D7')
        self.counter_label.grid(row=7, column=0, columnspan=5, pady=10, padx=0)
                
        # Variables to hold selected paths
        self.exclude_image_links = []
        self.output_folder = None
        self.image_excel_paths = []  # List of Excel files with image links
        self.exclude_excel_path = None
        self.radio_type = None
        self.counter_lock = threading.Lock()  # Lock for thread-safe counter access
        self.counters = defaultdict(int)  # Dictionary to maintain separate counters for each Excel file
        self.tempFilePath = "./tempfile.csv"
        self.failedLinks = []
        
        # Configure logging
        self.app_logger = AppLogging().get_logger()
    
    def show_exclude_button(self):
        self.exclude_excel_label.grid(row=3, column=0, padx=20, pady=10, sticky='w')
        self.exclude_button.grid(row=3, column=3, padx=10, pady=10, sticky='w')
    def hide_exclude_button(self):
        self.exclude_excel_label.grid_forget()
        self.exclude_button.grid_forget()
          
    def select_output_folder(self):
        self.output_folder = filedialog.askdirectory()
        print(self.output_folder)
        self.process_output.insert(tk.END, f"Selected Output Folder: {self.output_folder}\n")
        
    def select_image_excels(self):
        self.image_excel_paths = filedialog.askopenfilenames(filetypes=[("Excel files", "*.xlsx")])
        for path in self.image_excel_paths:
            self.process_output.insert(tk.END, f"Selected Image Excel: {path}\n")
            
    def select_exclude_excel(self):
        self.exclude_excel_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])
        self.process_output.insert(tk.END, f"Selected Exclude Excel: {self.exclude_excel_path}\n")
        
    def show_error_popup(self, message):
        messagebox.showerror("Error", message)     
        
    def show_info_popup(self, message):
        messagebox.showinfo("Info", message)

    def read_and_load_exclude_images(self):
        # Load excluded links
        sheets_dict = pd.read_excel(self.exclude_excel_path, sheet_name=None)
        # Combine all sheets into a single DataFrame
        exclude_df = pd.concat(sheets_dict.values(), ignore_index=True)
        # Extract exclusion lists
        exclude_list_links = set(exclude_df['EXCELUDE IMAGE'].dropna().tolist())  # Use a set for faster lookup
        exclude_links_and_names = list(zip(exclude_df['EXCELUDE IMAGE'].dropna().tolist(), exclude_df['NAME'].dropna().tolist()))
        self.exclude_image_links = list(exclude_list_links)
        return exclude_list_links, exclude_links_and_names
    
    def start_thread(self):
        try:
            num_threads = int(self.threads_entry.get())
            if num_threads <= 0:
                raise ValueError
        except ValueError:
            self.process_output.insert(tk.END, "Invalid number of threads. Please enter a positive integer.\n")
            return
        thread = threading.Thread(target=self.start_download, args=(num_threads,))
        thread.start()

    def start_download(self, num_threads):
        print("downloading is in progress")
        self.radio_type = self.type_selection.get().strip().lower()
        if self.radio_type == 1:
            if not all([self.output_folder, self.image_excel_paths, self.exclude_excel_path]) :
                self.process_output.insert(tk.END, "Please select all required files and folders.\n")
                return
        elif self.radio_type==2:
            if not all([self.output_folder, self.image_excel_paths, self.exclude_excel_path]) :
                self.process_output.insert(tk.END, "Please select all required files and folders.\n")
                return

        exclude_list_links=[] 
        exclude_links_and_names = []

        # Load excluded links
        if self.radio_type == 'type1':
            exclude_list_links, exclude_links_and_names = self.read_and_load_exclude_images()
        
        # Initialize counters
        total_links = 0
        
        # To remove duplicates based on the link
        seen_links = set()
        deduplicated_list = []                    
        image_tasks = []
        # iterating over each excel file one by one and download the image.
        for excel_path in self.image_excel_paths:
            image_df = pd.read_excel(excel_path)
            if self.radio_type == 'type1':
                links_and_names = list(zip(image_df['LINK IMAGE'].tolist(), image_df['NAME'].tolist()))    
                # excluding links that appear in exclude excel file.
                image_tasks.extend([(link, name, excel_path) for link, name in links_and_names if link not in exclude_list_links])
                                
            if self.radio_type == 'type2':
                links_and_names = list(zip(image_df['LINK IMAGE'].tolist(), image_df['NAME'].tolist()))                    
                image_tasks.extend([(link, name, excel_path) for link, name in links_and_names])
                self.exclude_image_links=[]
                
        if self.radio_type == 'type1':        
            for link, filename, _ in image_tasks:
                if link not in seen_links:
                    deduplicated_list.append((link, filename, _))
                    seen_links.add(link)        
            image_tasks = deduplicated_list 

                
        self.progress["maximum"] = len(image_tasks)
        total_links = len(image_tasks)
        
        # checking if the total number of images less than or equal to zero.
        if total_links <= 0:
            # self.app_logger.error("No image to download")
            self.show_error_popup("No image to download")
            self.progress["maximum"]=0
            return
        
        if len(image_tasks) > 0:
            # Divide tasks among threads
            task_sublists = self.divide_tasks(image_tasks, num_threads)
            threads = []
            for task in task_sublists:
                if self.radio_type == 'type1':
                    thread = threading.Thread(target=self.download_images, args=(task, exclude_links_and_names))
                    threads.append(thread)

                elif self.radio_type == 'type2':
                    self.exclude_image_links=[]
                    thread = threading.Thread(target=self.download_images, args=(task, []))
                    threads.append(thread)
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()
                                                        
            # downloading Failed Images 'LINK IMAGES', 'NAME','OUTPUT PATH'
            if os.path.exists(self.tempFilePath):
                df = pd.read_csv(self.tempFilePath)
                df = df.drop_duplicates()
                image_link, image_name, output_path = df['LINK IMAGE'].tolist(), df['NAME'].tolist(), df['OUTPUT PATH'].tolist()
                image_tasks = list(zip(image_link, image_name, output_path))
                
                # Divide tasks among threads
                task_sublists = self.divide_tasks(image_tasks, num_threads)

                # Create and start threads
                with ThreadPoolExecutor(max_workers=num_threads) as executor:
                    if self.radio_type == 'type1':
                        for task in task_sublists:
                            executor.submit(self.download_images, task, exclude_links_and_names, retried=True)
                    elif self.radio_type == 'type2':
                        self.exclude_image_links=[]
                        for task in task_sublists:
                            executor.submit(self.download_images, task, [], retried=True)
                            
                if os.path.exists(self.tempFilePath):
                    os.remove(self.tempFilePath)

            # Update the excluded links Excel file
            if exclude_links_and_names and self.radio_type == 'type1':
                updated_exclude_df = pd.DataFrame(list(exclude_links_and_names), columns=['EXCELUDE IMAGE', 'NAME'])
                if os.path.exists(self.exclude_excel_path):
                    # Read all sheets
                    sheets_dict = pd.read_excel(self.exclude_excel_path, sheet_name=None)
                else:
                    sheets_dict = {}
                    
                # Merge new data with existing data
                all_data = pd.concat(list(sheets_dict.values()) + [updated_exclude_df], ignore_index=True)
                
                # Drop duplicates and reset index
                all_data = all_data.drop_duplicates().reset_index(drop=True)
                
                # Split data into multiple sheets based on row cap
                max_rows = 1000000
                sheet_data = {}
                for i in range(0, len(all_data), max_rows):
                    sheet_name = f"Sheet_{(i // max_rows) + 1}"
                    sheet_data[sheet_name] = all_data.iloc[i:i + max_rows]

                # Write data to the Excel file
                with pd.ExcelWriter(self.exclude_excel_path, engine='openpyxl', mode='w') as writer:
                    for sheet_name, df in sheet_data.items():
                        df.to_excel(writer, sheet_name=sheet_name, index=False)

                print(f"Data saved across {len(sheet_data)} sheets successfully.")


        self.process_output.insert(tk.END, "Download completed.\n")
        self.show_info_popup("Download completed.\n")
        self.app_logger.critical("Download completed.")

    def divide_tasks(self, tasks, num_threads):
        """Divide the tasks evenly among the given number of threads."""
        avg = len(tasks) / float(num_threads)
        sublists = []
        last = 0.0

        while last < len(tasks):
            sublists.append(tasks[int(last):int(last + avg)])
            last += avg

        return sublists

    def sanitize_filename(self, name):
        """Remove symbols, emojis, and truncate the filename if it's too long."""
        name = str(name)
        # Remove emojis and non-alphanumeric characters except for spaces
        sanitized_name = re.sub(r'[^\w\s]', '', name)
        sanitized_name = re.sub(r'\s+', ' ', sanitized_name).strip()  # Replace multiple spaces with a single space

        # Truncate the name to the first 200 characters
        sanitized_name = sanitized_name[:200]

        return sanitized_name

    def ensure_unique_filename(self, filepath):
        """Ensure the filepath is unique by adding a number if necessary."""
        base, extension = os.path.splitext(filepath)
        counter = 1
        while os.path.exists(filepath):
            filepath = f"{base}_{counter}{extension}"
            counter += 1
        return filepath

    def download_images(self, tasks, exclude_links_and_names, retried=False):
        
        # Initialize WebDriver for this thread
        options = FirefoxOptions()
        options.add_argument('--lang=EN')
        options.set_preference('intl.accept_languages', 'en-US, en')
        options.add_argument('--headless')
        driver = webdriver.Firefox(options=options)

        # Set the window size to a large value for better quality screenshots
        driver.set_window_size(2560, 1440)

        for link, image_name, excel_path in tasks:
            if not retried:
                # Sanitize the image name            
                sanitized_name = self.sanitize_filename(image_name)

                folder_name = os.path.splitext(os.path.basename(excel_path))[0]
                folder_path = os.path.join(self.output_folder, folder_name)
                os.makedirs(folder_path, exist_ok=True)

                # Increment the counter for this Excel file safely
                with self.counter_lock:
                    self.counters[excel_path] += 1
                    local_counter = self.counters[excel_path]

                # Handle duplicate filenames by appending a number
                output_path = os.path.join(folder_path, f'{local_counter} {sanitized_name}.jpeg')
                output_path = self.ensure_unique_filename(output_path)
            else:
                output_path = excel_path
                sanitized_name = image_name
            if link not in self.exclude_image_links:
                try:
                    driver.get(link)
                    retries = 0
                    while True:
                        try:
                            retries+=1
                            if retries>=10:
                                break
                            img_element = driver.find_element(By.XPATH, '/html/body/img')
                            break
                        except:
                            print("Reloading Page")
                            driver.refresh()
                    png = img_element.screenshot_as_png  # Get the screenshot as a PNG binary

                    # Use Pillow to process the image and save as JPEG
                    image = Image.open(io.BytesIO(png))
                    image = image.convert('RGB')  # Convert to RGB mode for JPEG format
                    image.save(output_path, format='JPEG', quality=95, optimize=True)
                    if self.radio_type=='type1':
                        # Add the link to the set of excluded links
                        exclude_links_and_names.append((link,sanitized_name))
                        self.exclude_image_links.append(link)    

                    # updating progress bar 
                    self.progress.step(1)
                    current_progress = self.progress['value']
                    self.counter_label.config(text=f"{int(current_progress)}/{int(self.progress['maximum'])}")

                except Exception as e:
                    with threading.Lock():
                        self.process_output.insert(tk.END, f"Error saving image {link}: {str(e)}")
                        self.app_logger.error(f"Error saving image {link}: {str(e)}")
                        self.failedLinks.append((link,sanitized_name, output_path))

            else:
                self.app_logger.warning(f"Image already exists {link}")
   
                
        # Quit the driver after downloading is complete
        
        driver.quit()
        if len(self.failedLinks) > 0:
            with threading.Lock():
                df = pd.DataFrame(data=self.failedLinks, columns=['LINK IMAGE', 'NAME', 'OUTPUT PATH'])
                if os.path.exists(self.tempFilePath):
                    df.to_csv(self.tempFilePath, index=False, mode='a', header=False)
                else:
                    df.to_csv(self.tempFilePath, index=False, mode='a')
                self.failedLinks = []
            
if __name__ == "__main__":
    root = tk.Tk()
    app = ImageDownloaderApp(root)
    root.mainloop()
