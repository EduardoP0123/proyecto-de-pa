import tkinter as tk
from ui.ui_form import CSVUploaderApp

def main():
    root = tk.Tk()
    CSVUploaderApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()